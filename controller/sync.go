package controller

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/argoproj/gitops-engine/pkg/sync"
	"github.com/argoproj/gitops-engine/pkg/sync/common"
	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	cdcommon "github.com/argoproj/argo-cd/common"
	"github.com/argoproj/argo-cd/controller/metrics"
	"github.com/argoproj/argo-cd/pkg/apis/application/v1alpha1"
	listersv1alpha1 "github.com/argoproj/argo-cd/pkg/client/listers/application/v1alpha1"
	"github.com/argoproj/argo-cd/util/argo"
	logutils "github.com/argoproj/argo-cd/util/log"
	"github.com/argoproj/argo-cd/util/lua"
	"github.com/argoproj/argo-cd/util/rand"
)

var syncIdPrefix uint64 = 0

const (
	// EnvVarSyncWaveDelay is an environment variable which controls the delay in seconds between
	// each sync-wave
	EnvVarSyncWaveDelay = "ARGOCD_SYNC_WAVE_DELAY"
)

var jgwRemoveMe_nextInstanceId = 1

type syncContextEventLog struct {
	syncStartTime *time.Time
	appName       string
	oldWave       *int
	oldPhase      common.SyncPhase
	instanceId    int
}

func (t *syncContextEventLog) out(str ...interface{}) {

	currTime := ""

	if t.syncStartTime != nil {

		diff := time.Now().Sub(*t.syncStartTime)
		msecs := diff.Milliseconds()
		secs := diff.Seconds()

		currTime = fmt.Sprintf("%d.%03d", int64(secs), int32(msecs%1000))
	}

	outStr := fmt.Sprintln(str...)
	fmt.Printf("(%s)  [%v]  %v\n", currTime, t.appName, outStr)
}

func (t *syncContextEventLog) ActiveTasksInPhaseWave(phase common.SyncPhase, wave int) {

	if t.syncStartTime == nil {
		now := time.Now()
		t.syncStartTime = &now
	}

	// TODO: unit test this

	t.out("StartPhaseWave", phase, wave)

	// If:
	// - this oldPhase is empty because this is the first phase to run
	// - or, the phase has changed from last report
	if t.oldPhase == "" || phase != t.oldPhase {

		// If both the phase and wave were previously reported, and they don't match...
		if t.oldPhase != "" && t.oldWave != nil {
			// ... this implies a previous phase/wave ended
			t.PhaseWaveComplete(phase, wave, false)
		}

		// Report new phase
		t.out("🌙 Beginning phase", phase)
	}

	// If:
	// - we have not previously reported any waves as beginning
	// - or, the wave has changed
	// - or, the phase has changed
	if t.oldWave == nil || (wave != *t.oldWave) || phase != t.oldPhase {
		// ...then report a new wave
		t.out("🌊 Beginning wave", wave)
	}

	t.oldPhase = phase
	t.oldWave = &wave

}

func (t *syncContextEventLog) PhaseWaveComplete(newPhase common.SyncPhase, newWave int, final bool) {
	t.out("PhaseWaveComplete", newPhase, newWave, final)

	// TODO: unit test this

	// Report wave end iff:
	// - we have not previously reported a wave start
	// - or this is the final expected wave state change
	// - or the wave value has changed from the last reported value
	// - or the phase changed (implying the wave also ended)
	if final == true || t.oldWave == nil || *t.oldWave != newWave || t.oldPhase != newPhase {
		oldWave := ""
		if t.oldWave != nil {
			oldWave = fmt.Sprintf("%d", *t.oldWave)
		}
		t.out("🌊 Ending wave", oldWave)
	}

	// Report phase end iff:
	// - we have not previously reported a phase end
	// - or this is the final expected phase state change
	// - or the phase value has changed from the last reported value
	if final == true || (t.oldPhase != "" && t.oldPhase != newPhase) {
		t.out("🌙 Ending phase", t.oldPhase)
	}

}

func formatK8sResource(resource sync.ActionKubernetesResource) string {
	return fmt.Sprintf("'%s' (%s) %s", resource.Name, resource.Kind, resource.UID)
}

func (t *syncContextEventLog) ApplyResource(resource sync.ActionKubernetesResource) {
	t.out("Applying resource", formatK8sResource(resource))
}
func (t *syncContextEventLog) DeleteResource(resource sync.ActionKubernetesResource) {
	t.out("Deleting resource", formatK8sResource(resource))
}

func (t *syncContextEventLog) CreateTask(cdResource sync.ActionCDResource, k8sResource sync.ActionKubernetesResource) {

	if cdResource.HookType != "" {
		// Hook
		t.out(fmt.Sprintf("Creating Hook %s🪝 - %s", cdResource.HookType, formatK8sResource(k8sResource)))

	} else {
		// !Hook
		t.out(fmt.Sprintf("Creating %s", formatK8sResource(k8sResource)))
	}

}
func (t *syncContextEventLog) PruneTask(cdResource sync.ActionCDResource, k8sResource sync.ActionKubernetesResource) {

	if cdResource.HookType != "" {
		// Hook
		t.out(fmt.Sprintf("Pruning Hook %s🪝 - %s", cdResource.HookType, formatK8sResource(k8sResource)))

	} else {

		// !Hook
		t.out(fmt.Sprintf("Pruning %s", formatK8sResource(k8sResource)))
	}

}
func (t *syncContextEventLog) DeleteHook(cdResource sync.ActionCDResource, k8sResource sync.ActionKubernetesResource) {

	// Hook
	t.out(fmt.Sprintf("Deleting Hook %s🪝 - %s", cdResource.HookType, formatK8sResource(k8sResource)))
}

// func (t *syncContextEventLog) DeleteResource(name string, kind string, uid string) {
// 	t.out("Deleting resource", fmt.Sprintf("'%s' (%s) %s", name, kind, uid))
// }

// func (t *syncContextEventLog) ApplyResource(name string, kind string, uid string) {
// 	t.out("Applying resource", fmt.Sprintf("'%s' (%s) %s", name, kind, uid))
// }

type hi struct {
	name      string
	kind      string
	uid       string
	namespace string
}

var _ sync.SyncContextEventLog = &syncContextEventLog{}

func (m *appStateManager) SyncAppState(app *v1alpha1.Application, state *v1alpha1.OperationState) {
	// Sync requests might be requested with ambiguous revisions (e.g. master, HEAD, v1.2.3).
	// This can change meaning when resuming operations (e.g a hook sync). After calculating a
	// concrete git commit SHA, the SHA is remembered in the status.operationState.syncResult field.
	// This ensures that when resuming an operation, we sync to the same revision that we initially
	// started with.
	var revision string
	var syncOp v1alpha1.SyncOperation
	var syncRes *v1alpha1.SyncOperationResult
	var source v1alpha1.ApplicationSource

	if state.Operation.Sync == nil {
		state.Phase = common.OperationFailed
		state.Message = "Invalid operation request: no operation specified"
		return
	}
	syncOp = *state.Operation.Sync
	if syncOp.Source == nil {
		// normal sync case (where source is taken from app.spec.source)
		source = app.Spec.Source
	} else {
		// rollback case
		source = *state.Operation.Sync.Source
	}

	if state.SyncResult != nil {
		syncRes = state.SyncResult
		revision = state.SyncResult.Revision
	} else {
		syncRes = &v1alpha1.SyncOperationResult{}
		// status.operationState.syncResult.source. must be set properly since auto-sync relies
		// on this information to decide if it should sync (if source is different than the last
		// sync attempt)
		syncRes.Source = source
		state.SyncResult = syncRes
	}

	if revision == "" {
		// if we get here, it means we did not remember a commit SHA which we should be syncing to.
		// This typically indicates we are just about to begin a brand new sync/rollback operation.
		// Take the value in the requested operation. We will resolve this to a SHA later.
		revision = syncOp.Revision
	}

	proj, err := argo.GetAppProject(&app.Spec, listersv1alpha1.NewAppProjectLister(m.projInformer.GetIndexer()), m.namespace, m.settingsMgr)
	if err != nil {
		state.Phase = common.OperationError
		state.Message = fmt.Sprintf("Failed to load application project: %v", err)
		return
	}

	compareResult := m.CompareAppState(app, proj, revision, source, false, syncOp.Manifests)
	// We now have a concrete commit SHA. Save this in the sync result revision so that we remember
	// what we should be syncing to when resuming operations.
	syncRes.Revision = compareResult.syncStatus.Revision

	// If there are any comparison or spec errors error conditions do not perform the operation
	if errConditions := app.Status.GetConditions(map[v1alpha1.ApplicationConditionType]bool{
		v1alpha1.ApplicationConditionComparisonError:  true,
		v1alpha1.ApplicationConditionInvalidSpecError: true,
	}); len(errConditions) > 0 {
		state.Phase = common.OperationError
		state.Message = argo.FormatAppConditions(errConditions)
		return
	}

	clst, err := m.db.GetCluster(context.Background(), app.Spec.Destination.Server)
	if err != nil {
		state.Phase = common.OperationError
		state.Message = err.Error()
		return
	}

	rawConfig := clst.RawRestConfig()
	restConfig := metrics.AddMetricsTransportWrapper(m.metricsServer, app, clst.RESTConfig())

	resourceOverrides, err := m.settingsMgr.GetResourceOverrides()
	if err != nil {
		state.Phase = common.OperationError
		state.Message = fmt.Sprintf("Failed to load resource overrides: %v", err)
		return
	}

	atomic.AddUint64(&syncIdPrefix, 1)
	syncId := fmt.Sprintf("%05d-%s", syncIdPrefix, rand.RandString(5))

	logEntry := log.WithFields(log.Fields{"application": app.Name, "syncId": syncId})
	initialResourcesRes := make([]common.ResourceSyncResult, 0)
	for i, res := range syncRes.Resources {
		key := kube.ResourceKey{Group: res.Group, Kind: res.Kind, Namespace: res.Namespace, Name: res.Name}
		initialResourcesRes = append(initialResourcesRes, common.ResourceSyncResult{
			ResourceKey: key,
			Message:     res.Message,
			Status:      res.Status,
			HookPhase:   res.HookPhase,
			HookType:    res.HookType,
			SyncPhase:   res.SyncPhase,
			Version:     res.Version,
			Order:       i + 1,
		})
	}

	appEntry := m.sessionCache.getOrCreate(app.UID)

	if appEntry.eventLog == nil {

		var eventContextLog *syncContextEventLog = &syncContextEventLog{
			appName:    app.Name,
			instanceId: jgwRemoveMe_nextInstanceId,
		}
		jgwRemoveMe_nextInstanceId++

		appEntry.eventLog = eventContextLog

	}

	// fmt.Println("jgw IN SyncAppState", appEntry.eventLog.instanceId)
	// defer fmt.Println("jgw OUT SyncAppState", appEntry.eventLog.instanceId)

	syncCtx, err := sync.NewSyncContext(
		compareResult.syncStatus.Revision,
		compareResult.reconciliationResult,
		restConfig,
		rawConfig,
		m.kubectl,
		app.Spec.Destination.Namespace,
		appEntry.eventLog,
		sync.WithLogr(logutils.NewLogrusLogger(logEntry)),
		sync.WithHealthOverride(lua.ResourceHealthOverrides(resourceOverrides)),
		sync.WithPermissionValidator(func(un *unstructured.Unstructured, res *v1.APIResource) error {
			if !proj.IsGroupKindPermitted(un.GroupVersionKind().GroupKind(), res.Namespaced) {
				return fmt.Errorf("Resource %s:%s is not permitted in project %s.", un.GroupVersionKind().Group, un.GroupVersionKind().Kind, proj.Name)
			}
			if res.Namespaced && !proj.IsDestinationPermitted(v1alpha1.ApplicationDestination{Namespace: un.GetNamespace(), Server: app.Spec.Destination.Server}) {
				return fmt.Errorf("namespace %v is not permitted in project '%s'", un.GetNamespace(), proj.Name)
			}
			return nil
		}),
		sync.WithOperationSettings(syncOp.DryRun, syncOp.Prune, syncOp.SyncStrategy.Force(), syncOp.IsApplyStrategy() || len(syncOp.Resources) > 0),
		sync.WithInitialState(state.Phase, state.Message, initialResourcesRes, state.StartedAt),
		sync.WithResourcesFilter(func(key kube.ResourceKey, target *unstructured.Unstructured, live *unstructured.Unstructured) bool {
			return len(syncOp.Resources) == 0 || argo.ContainsSyncResource(key.Name, key.Namespace, schema.GroupVersionKind{Kind: key.Kind, Group: key.Group}, syncOp.Resources)
		}),
		sync.WithManifestValidation(!syncOp.SyncOptions.HasOption("Validate=false")),
		sync.WithNamespaceCreation(syncOp.SyncOptions.HasOption("CreateNamespace=true"), func(un *unstructured.Unstructured) bool {
			if un != nil && kube.GetAppInstanceLabel(un, cdcommon.LabelKeyAppInstance) != "" {
				kube.UnsetLabel(un, cdcommon.LabelKeyAppInstance)
				return true
			}
			return false
		}),
		sync.WithSyncWaveHook(delayBetweenSyncWaves),
	)

	if err != nil {
		state.Phase = common.OperationError
		state.Message = fmt.Sprintf("failed to record sync to history: %v", err)
	}

	start := time.Now()

	if state.Phase == common.OperationTerminating {
		fmt.Println("Terminate called from controller/sync.go")
		syncCtx.Terminate()
	} else {
		syncCtx.Sync()
	}
	var resState []common.ResourceSyncResult
	state.Phase, state.Message, resState = syncCtx.GetState()
	state.SyncResult.Resources = nil
	for _, res := range resState {
		state.SyncResult.Resources = append(state.SyncResult.Resources, &v1alpha1.ResourceResult{
			HookType:  res.HookType,
			Group:     res.ResourceKey.Group,
			Kind:      res.ResourceKey.Kind,
			Namespace: res.ResourceKey.Namespace,
			Name:      res.ResourceKey.Name,
			Version:   res.Version,
			SyncPhase: res.SyncPhase,
			HookPhase: res.HookPhase,
			Status:    res.Status,
			Message:   res.Message,
		})
	}

	logEntry.WithField("duration", time.Since(start)).Info("sync/terminate complete")

	if !syncOp.DryRun && len(syncOp.Resources) == 0 && state.Phase.Successful() {
		err := m.persistRevisionHistory(app, compareResult.syncStatus.Revision, source, state.StartedAt)
		if err != nil {
			state.Phase = common.OperationError
			state.Message = fmt.Sprintf("failed to record sync to history: %v", err)
		}
	}
}

// delayBetweenSyncWaves is a gitops-engine SyncWaveHook which introduces an artificial delay
// between each sync wave. We introduce an artificial delay in order give other controllers a
// _chance_ to react to the spec change that we just applied. This is important because without
// this, Argo CD will likely assess resource health too quickly (against the stale object), causing
// hooks to fire prematurely. See: https://github.com/argoproj/argo-cd/issues/4669.
// Note, this is not foolproof, since a proper fix would require the CRD record
// status.observedGeneration coupled with a health.lua that verifies
// status.observedGeneration == metadata.generation
func delayBetweenSyncWaves(phase common.SyncPhase, wave int, finalWave bool) error {
	if !finalWave {
		delaySec := 2
		if delaySecStr := os.Getenv(EnvVarSyncWaveDelay); delaySecStr != "" {
			if val, err := strconv.Atoi(delaySecStr); err == nil {
				delaySec = val
			}
		}
		duration := time.Duration(delaySec) * time.Second
		time.Sleep(duration)
	}
	return nil
}
