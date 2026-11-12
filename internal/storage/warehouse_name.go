package storage

import "sync/atomic"

// warehouseRootName is the name of the Iceberg warehouse directory directly under the storage
// root, when iceberg.warehouse is a subdirectory of it ("<root>/lv_wh" → "lv_wh"), or nil.
var warehouseRootName atomic.Pointer[string]

// SetWarehouseRootName records the Iceberg warehouse's directory name under the storage root, so
// that every writer refuses a database of that name: its files would land inside the warehouse
// (#1626). Set once at startup, from the exporter's layout; "" clears it.
func SetWarehouseRootName(name string) {
	if name == "" {
		warehouseRootName.Store(nil)
		return
	}
	warehouseRootName.Store(&name)
}

// IsWarehouseRootName reports whether a database name is the Iceberg warehouse's directory under
// the storage root. Only a subdirectory warehouse has such a name: a namespace directory's name
// carries a dot, which no database name may. The comparison is exact; on a volume that folds
// names, another spelling of an existing directory is refused by the name-spelling guard.
func IsWarehouseRootName(name string) bool {
	p := warehouseRootName.Load()
	return p != nil && *p == name
}
