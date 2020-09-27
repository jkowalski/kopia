package server

import (
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func (s *Server) handleObjectGet(w http.ResponseWriter, r *http.Request) {
	oidstr := mux.Vars(r)["objectID"]

	oid, err := object.ParseID(oidstr)
	if err != nil {
		http.Error(w, "invalid object id", http.StatusBadRequest)
		return
	}

	obj, err := s.rep.OpenObject(r.Context(), oid)
	if errors.Is(err, object.ErrObjectNotFound) {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	if snapshotfs.IsDirectoryID(oid) {
		w.Header().Set("Content-Type", "application/json")
	}

	fname := oid.String()
	if p := r.URL.Query().Get("fname"); p != "" {
		fname = p
		w.Header().Set("Content-Disposition", "attachment; filename=\""+p+"\"")
	}

	mtime := clock.Now()

	if p := r.URL.Query().Get("mtime"); p != "" {
		if m, err := time.Parse(time.RFC3339Nano, p); err == nil {
			mtime = m
		}
	}

	http.ServeContent(w, r, fname, mtime, obj)
}
