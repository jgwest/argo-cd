package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	. "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/test/e2e/fixture"
	. "github.com/argoproj/argo-cd/v3/test/e2e/fixture/app"
	"github.com/argoproj/argo-cd/v3/util/errors"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

// TODO: Move this test to a proper spot (another _test.go file)

// When an Argo CD Application has a '.spec.target.source.targetRevision' that references an annotated tag, the `.status.sync.revision` field should be the Git commit SHA, NOT the annotated tag ID.
// - See this URL for the difference between the two IDs: https://stackoverflow.com/questions/39754373/why-are-there-two-different-git-commits-for-the-same-tag/39757198#39757198
func TestAnnotatedTagInStatusSyncRevision(t *testing.T) {
	Given(t).
		Path(guestbookPath).
		When().
		// Create annotated tag name 'annotated-tag'
		AddAnnotatedTag("annotated-tag", "my-generic-tag-message").
		// Create Application targetting annotated-tag, with automatedSync: true
		CreateFromFile(func(app *Application) {
			app.Spec.Source.TargetRevision = "annotated-tag"
			app.Spec.SyncPolicy = &SyncPolicy{Automated: &SyncPolicyAutomated{Prune: true, SelfHeal: false}}
		}).
		Then().
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		And(func(app *Application) {

			annotatedTagIDOutput, err := fixture.Run(fixture.TmpDir+"/testdata.git", "git", "show-ref", "annotated-tag")
			require.NoError(t, err)
			require.NotEmpty(t, annotatedTagIDOutput)
			// example command output:
			// "569798c430515ffe170bdb23e3aafaf8ae24b9ff refs/tags/annotated-tag"
			annotatedTagIDFields := strings.Fields(string(annotatedTagIDOutput))
			require.Equal(t, len(annotatedTagIDFields), 2)

			targetCommitID, err := fixture.Run(fixture.TmpDir+"/testdata.git", "git", "rev-parse", "--verify", "annotated-tag^{commit}")
			// example command output:
			// "bcd35965e494273355265b9f0bf85075b6bc5163"
			require.NoError(t, err)
			require.NotEmpty(t, targetCommitID)

			require.NotEmpty(t, app.Status.Sync.Revision, "revision in sync status should be set by sync operation")

			require.NotEqual(t, app.Status.Sync.Revision, annotatedTagIDFields[0], "revision should not match the annotated tag id")
			require.Equal(t, app.Status.Sync.Revision, strings.TrimSpace(string(targetCommitID)), "revision SHOULD match the target commit SHA")
		})
}

// When:
// - an Argo CD Application has a '.spec.target.source.targetRevision' that references an annotated tag,
// - AND
// - the Argo CD Application has automated sync policy, but self heal false.
// THEN
// - updates to K8s resources should not trigger a self-heal.

// - See this URL for the difference between the two IDs: https://stackoverflow.com/questions/39754373/why-are-there-two-different-git-commits-for-the-same-tag/39757198#39757198

func TestAutomatedSelfHealingAgainstAnnotatedTag(t *testing.T) {

	Given(t).
		Path(guestbookPath).
		When().
		AddAnnotatedTag("annotated-tag", "my-generic-tag-message").
		// App should be auto-synced once created
		CreateFromFile(func(app *Application) {
			app.Spec.Source.TargetRevision = "annotated-tag"
			app.Spec.SyncPolicy = &SyncPolicy{Automated: &SyncPolicyAutomated{Prune: true, SelfHeal: false}}
		}).
		Then().
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		ExpectConsistently(SyncStatusIs(SyncStatusCodeSynced), WaitDuration, time.Second*10).
		When().
		// Update the annotated tag to a new git commit, that has a new revisionHistoryLimit.
		PatchFile("guestbook-ui-deployment.yaml", `[{"op": "replace", "path": "/spec/revisionHistoryLimit", "value": 10}]`).
		AddAnnotatedTag("annotated-tag", "my-generic-tag-message").
		Refresh(RefreshTypeHard).
		// The Application should update to the new annotated tag value within 10 seconds.
		And(func() {

			// Deployment revisionHistoryLimit should switch to 10
			timeoutErr := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 10*time.Second, true, func(context.Context) (done bool, err error) {

				deployment, err := fixture.KubeClientset.AppsV1().Deployments(fixture.DeploymentNamespace()).Get(context.Background(), "guestbook-ui", metav1.GetOptions{})

				if err != nil {
					return false, nil
				}

				revisionHistoryLimit := deployment.Spec.RevisionHistoryLimit
				return revisionHistoryLimit != nil && *revisionHistoryLimit == 10, nil

			})
			require.NoError(t, timeoutErr)
		}).
		// Update the Deployment to a different revisionHistoryLimit
		And(func() {
			errors.FailOnErr(fixture.KubeClientset.AppsV1().Deployments(fixture.DeploymentNamespace()).Patch(context.Background(),
				"guestbook-ui", types.MergePatchType, []byte(`{"spec": {"revisionHistoryLimit": 9}}`), metav1.PatchOptions{}))
		}).
		// The revisionHistoryLimit should NOT be self-healed, because selfHealing: false. It should remain at 9.
		And(func() {

			// Wait up to 10 seconds to ensure that deployment revisionHistoryLimit does NOT should switch to 10, it should remain at 9.
			waitErr := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 10*time.Second, true, func(context.Context) (done bool, err error) {

				deployment, err := fixture.KubeClientset.AppsV1().Deployments(fixture.DeploymentNamespace()).Get(context.Background(), "guestbook-ui", metav1.GetOptions{})

				if err != nil {
					return false, nil
				}

				revisionHistoryLimit := deployment.Spec.RevisionHistoryLimit
				return revisionHistoryLimit != nil && *revisionHistoryLimit != 9, nil

			})
			require.Error(t, waitErr, "A timeout error should occur, indicating that revisionHistoryLimit never changed from 9")
		})
}

func TestAutomatedSelfHealingAgainstLightweightTag(t *testing.T) {

	Given(t).
		Path(guestbookPath).
		When().
		AddTag("annotated-tag").
		// App should be auto-synced once created
		CreateFromFile(func(app *Application) {
			app.Spec.Source.TargetRevision = "annotated-tag"
			app.Spec.SyncPolicy = &SyncPolicy{Automated: &SyncPolicyAutomated{Prune: true, SelfHeal: false}}
		}).
		Then().
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		ExpectConsistently(SyncStatusIs(SyncStatusCodeSynced), WaitDuration, time.Second*10).
		When().
		// Update the annotated tag to a new git commit, that has a new revisionHistoryLimit.
		PatchFile("guestbook-ui-deployment.yaml", `[{"op": "replace", "path": "/spec/revisionHistoryLimit", "value": 10}]`).
		AddTagWithForce("annotated-tag").
		Refresh(RefreshTypeHard).
		// The Application should update to the new annotated tag value within 10 seconds.
		And(func() {

			// Deployment revisionHistoryLimit should switch to 10
			timeoutErr := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 10*time.Second, true, func(context.Context) (done bool, err error) {

				deployment, err := fixture.KubeClientset.AppsV1().Deployments(fixture.DeploymentNamespace()).Get(context.Background(), "guestbook-ui", metav1.GetOptions{})

				if err != nil {
					return false, nil
				}

				revisionHistoryLimit := deployment.Spec.RevisionHistoryLimit
				return revisionHistoryLimit != nil && *revisionHistoryLimit == 10, nil

			})
			require.NoError(t, timeoutErr)
		}).
		// Update the Deployment to a different revisionHistoryLimit
		And(func() {
			errors.FailOnErr(fixture.KubeClientset.AppsV1().Deployments(fixture.DeploymentNamespace()).Patch(context.Background(),
				"guestbook-ui", types.MergePatchType, []byte(`{"spec": {"revisionHistoryLimit": 9}}`), metav1.PatchOptions{}))
		}).
		// The revisionHistoryLimit should NOT be self-healed, because selfHealing: false
		And(func() {

			// Wait up to 10 seconds to ensure that deployment revisionHistoryLimit does NOT should switch to 10, it should remain at 9.
			waitErr := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 10*time.Second, true, func(context.Context) (done bool, err error) {

				deployment, err := fixture.KubeClientset.AppsV1().Deployments(fixture.DeploymentNamespace()).Get(context.Background(), "guestbook-ui", metav1.GetOptions{})

				if err != nil {
					return false, nil
				}

				revisionHistoryLimit := deployment.Spec.RevisionHistoryLimit
				return revisionHistoryLimit != nil && *revisionHistoryLimit != 9, nil

			})
			require.Error(t, waitErr, "A timeout error should occur, indicating that revisionHistoryLimit never changed from 9")
		})
}
