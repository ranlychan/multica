package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func startHatchet(ctx context.Context, c hatchet.Config, pool *pgxpool.Pool) (*hatchet.Supervisor, *hatchet.TenantService, error) {
	if !c.Enabled {
		s := hatchet.NewSupervisor(c, nil)
		return s, nil, s.Start(ctx)
	}
	key, err := secretbox.LoadKey("MULTICA_HATCHET_SECRET_KEY")
	if err != nil {
		return nil, nil, errors.New("hatchet: valid MULTICA_HATCHET_SECRET_KEY required")
	}
	box, err := secretbox.New(key)
	if err != nil {
		return nil, nil, errors.New("hatchet: secret key invalid")
	}
	client, err := hatchet.NewClient(c.APIURL, nil)
	if err != nil {
		return nil, nil, err
	}
	tenants, err := hatchet.NewSQLTenantService(client, db.New(pool), pool, box, c.ProvisioningEmail, c.ProvisioningPassword)
	if err != nil {
		return nil, nil, err
	}
	s := hatchet.NewSupervisor(c, func(ctx context.Context) error {
		err := tenants.CleanupPending(ctx)
		if err != nil {
			slog.Warn("workflow tenant cleanup postponed; retry receipts retained")
		}
		return err
	})
	if err := s.Start(ctx); err != nil {
		return nil, nil, err
	}
	return s, tenants, nil
}
