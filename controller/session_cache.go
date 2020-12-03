package controller

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// For caching values that:
// - are not important enough to put in redis
// - will no longer be relevant if the controller process were to restart (eg data is no longer needed if the controller were to restart)
// threadsafe
type sessionCache struct {

	// map key is Application UID
	applications map[string]*applicationEntry

	lock sync.Mutex
}

func newSessionCache() *sessionCache {
	return &sessionCache{
		applications: map[string]*applicationEntry{},
	}
}

type applicationEntry struct {
	expirationTime time.Time

	eventLog *syncContextEventLog

	// values map[string]interface{}
	// obj interface{}
}

// prune removes old applications from the application map, if their expiration deadline has been met
// Ensure sh.lock is owned by the caller before calling prune()
func (sh *sessionCache) prune() {
	for uid, app := range sh.applications {
		if time.Now().After(app.expirationTime) {
			delete(sh.applications, uid)
		}
	}
}

func (sh *sessionCache) getOrCreate(uid types.UID) *applicationEntry {
	sh.lock.Lock()
	defer sh.lock.Unlock()

	if val, exists := sh.applications[string(uid)]; exists {
		// keep alive any values that are used
		val.renewExpiration()
		return val
	}

	// prune old values before creating a new value
	sh.prune()

	appEntry := &applicationEntry{
		expirationTime: time.Now(),
	}
	appEntry.renewExpiration()

	sh.applications[string(uid)] = appEntry

	return appEntry

}

// func (appEntry *applicationEntry) object() interface{} {
// 	return appEntry.obj
// }

func (appEntry *applicationEntry) renewExpiration() {
	appEntry.expirationTime = appEntry.expirationTime.Add(12 * time.Hour)
}
