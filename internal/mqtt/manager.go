package mqtt

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/shutdown"
)

// Manager interface for MQTT subscription management
type Manager interface {
	// Lifecycle
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error

	// CRUD operations
	Create(ctx context.Context, req *CreateSubscriptionRequest, password string) (*Subscription, error)
	Get(ctx context.Context, id string) (*Subscription, error)
	List(ctx context.Context) ([]*Subscription, error)
	Update(ctx context.Context, id string, req *UpdateSubscriptionRequest) (*Subscription, error)
	Delete(ctx context.Context, id string) error

	// Subscription lifecycle
	StartSubscription(ctx context.Context, id string) error
	StopSubscription(ctx context.Context, id string) error
	PauseSubscription(ctx context.Context, id string) error
	GetStats(ctx context.Context, id string) (*SubscriptionStats, error)
	GetAllStats(ctx context.Context) ([]*SubscriptionStats, error)
}

// SubscriptionManager manages multiple MQTT subscribers
type SubscriptionManager struct {
	repo        *Repository
	encryptor   PasswordEncryptor
	arrowBuffer *ingest.ArrowBuffer
	logger      zerolog.Logger

	// Running subscribers
	mu          sync.RWMutex
	subscribers map[string]*Subscriber
}

// Ensure SubscriptionManager implements Manager and Shutdownable
var _ Manager = (*SubscriptionManager)(nil)
var _ shutdown.Shutdownable = (*SubscriptionManager)(nil)

// NewSubscriptionManager creates a new subscription manager
func NewSubscriptionManager(
	repo *Repository,
	encryptor PasswordEncryptor,
	arrowBuffer *ingest.ArrowBuffer,
	logger zerolog.Logger,
) *SubscriptionManager {
	return &SubscriptionManager{
		repo:        repo,
		encryptor:   encryptor,
		arrowBuffer: arrowBuffer,
		logger:      logger.With().Str("component", "mqtt-manager").Logger(),
		subscribers: make(map[string]*Subscriber),
	}
}

// Start initializes the manager and starts auto-start subscriptions
func (m *SubscriptionManager) Start(ctx context.Context) error {
	m.logger.Info().Msg("Starting MQTT subscription manager")

	// Load and start auto-start subscriptions
	subscriptions, err := m.repo.ListAutoStart(ctx)
	if err != nil {
		return fmt.Errorf("failed to load auto-start subscriptions: %w", err)
	}

	m.logger.Info().Int("count", len(subscriptions)).Msg("Found auto-start subscriptions")

	for _, sub := range subscriptions {
		if err := m.startSubscriber(sub); err != nil {
			m.logger.Error().
				Err(err).
				Str("id", sub.ID).
				Str("name", sub.Name).
				Msg("Failed to start auto-start subscription")

			// Update status to error
			m.repo.UpdateStatus(ctx, sub.ID, StatusError, err.Error())
		}
	}

	return nil
}

// Close implements shutdown.Shutdownable interface
func (m *SubscriptionManager) Close() error {
	return m.Shutdown(context.Background())
}

// Shutdown stops all subscribers and closes resources
func (m *SubscriptionManager) Shutdown(ctx context.Context) error {
	m.logger.Info().Msg("Shutting down MQTT subscription manager")

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop all subscribers
	for id, subscriber := range m.subscribers {
		if err := subscriber.Stop(); err != nil {
			m.logger.Error().Err(err).Str("id", id).Msg("Error stopping subscriber during shutdown")
		}
	}

	m.subscribers = make(map[string]*Subscriber)

	// Close repository
	if err := m.repo.Close(); err != nil {
		m.logger.Error().Err(err).Msg("Error closing repository during shutdown")
	}

	m.logger.Info().Msg("MQTT subscription manager shutdown complete")
	return nil
}

// Create creates a new subscription
func (m *SubscriptionManager) Create(ctx context.Context, req *CreateSubscriptionRequest, password string) (*Subscription, error) {
	// Validate request
	if err := ValidateCreateRequest(req); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	// Encrypt password if provided
	var encryptedPassword string
	if password != "" {
		var err error
		encryptedPassword, err = m.encryptor.Encrypt(password)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt password: %w", err)
		}
	}

	// Create subscription
	sub := &Subscription{
		Name:                  req.Name,
		Broker:                req.Broker,
		ClientID:              req.ClientID,
		Topics:                req.Topics,
		QoS:                   req.QoS,
		Database:              req.Database,
		Username:              req.Username,
		PasswordEncrypted:     encryptedPassword,
		HasPassword:           password != "",
		TLSEnabled:            req.TLSEnabled,
		TLSCertPath:           req.TLSCertPath,
		TLSKeyPath:            req.TLSKeyPath,
		TLSCAPath:             req.TLSCAPath,
		TLSInsecureSkipVerify: req.TLSInsecureSkipVerify,
		AutoStart:             req.AutoStart,
		TopicMapping:          req.TopicMapping,
		KeepAliveSeconds:      req.KeepAliveSeconds,
		ConnectTimeoutSeconds: req.ConnectTimeoutSeconds,
		ReconnectMinSeconds:   req.ReconnectMinSeconds,
		ReconnectMaxSeconds:   req.ReconnectMaxSeconds,
		Status:                StatusStopped,
	}

	sub.SetDefaults()

	if err := m.repo.Create(ctx, sub); err != nil {
		return nil, err
	}

	m.logger.Info().
		Str("id", sub.ID).
		Str("name", sub.Name).
		Str("broker", sub.Broker).
		Strs("topics", sub.Topics).
		Msg("Created MQTT subscription")

	return sub, nil
}

// Get retrieves a subscription by ID
func (m *SubscriptionManager) Get(ctx context.Context, id string) (*Subscription, error) {
	sub, err := m.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, fmt.Errorf("subscription not found: %s", id)
	}
	return sub, nil
}

// List retrieves all subscriptions
func (m *SubscriptionManager) List(ctx context.Context) ([]*Subscription, error) {
	return m.repo.List(ctx)
}

// Update updates an existing subscription
func (m *SubscriptionManager) Update(ctx context.Context, id string, req *UpdateSubscriptionRequest) (*Subscription, error) {
	// Get existing subscription
	sub, err := m.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, fmt.Errorf("subscription not found: %s", id)
	}

	// Check if running - must stop first
	m.mu.RLock()
	_, isRunning := m.subscribers[id]
	m.mu.RUnlock()

	if isRunning {
		return nil, fmt.Errorf("cannot update running subscription - stop it first")
	}

	// Apply updates
	if req.Name != nil {
		sub.Name = *req.Name
	}
	if req.Broker != nil {
		sub.Broker = *req.Broker
	}
	if req.ClientID != nil {
		sub.ClientID = *req.ClientID
	}
	if req.Topics != nil {
		sub.Topics = req.Topics
	}
	if req.QoS != nil {
		sub.QoS = *req.QoS
	}
	if req.Database != nil {
		sub.Database = *req.Database
	}
	if req.Username != nil {
		sub.Username = *req.Username
	}
	if req.Password != nil {
		if *req.Password != "" {
			encrypted, err := m.encryptor.Encrypt(*req.Password)
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt password: %w", err)
			}
			sub.PasswordEncrypted = encrypted
			sub.HasPassword = true
		} else {
			sub.PasswordEncrypted = ""
			sub.HasPassword = false
		}
	}
	if req.TLSEnabled != nil {
		sub.TLSEnabled = *req.TLSEnabled
	}
	if req.TLSCertPath != nil {
		sub.TLSCertPath = *req.TLSCertPath
	}
	if req.TLSKeyPath != nil {
		sub.TLSKeyPath = *req.TLSKeyPath
	}
	if req.TLSCAPath != nil {
		sub.TLSCAPath = *req.TLSCAPath
	}
	if req.TLSInsecureSkipVerify != nil {
		sub.TLSInsecureSkipVerify = *req.TLSInsecureSkipVerify
	}
	if req.AutoStart != nil {
		sub.AutoStart = *req.AutoStart
	}
	if req.TopicMapping != nil {
		sub.TopicMapping = *req.TopicMapping
	}
	if req.KeepAliveSeconds != nil {
		sub.KeepAliveSeconds = *req.KeepAliveSeconds
	}
	if req.ConnectTimeoutSeconds != nil {
		sub.ConnectTimeoutSeconds = *req.ConnectTimeoutSeconds
	}
	if req.ReconnectMinSeconds != nil {
		sub.ReconnectMinSeconds = *req.ReconnectMinSeconds
	}
	if req.ReconnectMaxSeconds != nil {
		sub.ReconnectMaxSeconds = *req.ReconnectMaxSeconds
	}

	// Validate updated subscription
	if err := sub.Validate(); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	if err := m.repo.Update(ctx, sub); err != nil {
		return nil, err
	}

	m.logger.Info().Str("id", id).Str("name", sub.Name).Msg("Updated MQTT subscription")

	return sub, nil
}

// Delete deletes a subscription
func (m *SubscriptionManager) Delete(ctx context.Context, id string) error {
	// Stop if running
	m.mu.Lock()
	if subscriber, ok := m.subscribers[id]; ok {
		subscriber.Stop()
		delete(m.subscribers, id)
	}
	m.mu.Unlock()

	if err := m.repo.Delete(ctx, id); err != nil {
		return err
	}

	m.logger.Info().Str("id", id).Msg("Deleted MQTT subscription")
	return nil
}

// StartSubscription starts a subscription
func (m *SubscriptionManager) StartSubscription(ctx context.Context, id string) error {
	sub, err := m.repo.Get(ctx, id)
	if err != nil {
		return err
	}
	if sub == nil {
		return fmt.Errorf("subscription not found: %s", id)
	}

	m.mu.Lock()
	if _, ok := m.subscribers[id]; ok {
		m.mu.Unlock()
		return fmt.Errorf("subscription already running")
	}
	m.mu.Unlock()

	if err := m.startSubscriber(sub); err != nil {
		m.repo.UpdateStatus(ctx, id, StatusError, err.Error())
		return err
	}

	return nil
}

// StopSubscription stops a subscription
func (m *SubscriptionManager) StopSubscription(ctx context.Context, id string) error {
	m.mu.Lock()
	subscriber, ok := m.subscribers[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("subscription not running")
	}
	delete(m.subscribers, id)
	m.mu.Unlock()

	if err := subscriber.Stop(); err != nil {
		return err
	}

	m.repo.UpdateStatus(ctx, id, StatusStopped, "")
	m.logger.Info().Str("id", id).Msg("Stopped MQTT subscription")

	return nil
}

// PauseSubscription pauses a subscription (stops without clearing error state)
func (m *SubscriptionManager) PauseSubscription(ctx context.Context, id string) error {
	m.mu.Lock()
	subscriber, ok := m.subscribers[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("subscription not running")
	}
	delete(m.subscribers, id)
	m.mu.Unlock()

	if err := subscriber.Stop(); err != nil {
		return err
	}

	m.repo.UpdateStatus(ctx, id, StatusPaused, "")
	m.logger.Info().Str("id", id).Msg("Paused MQTT subscription")

	return nil
}

// GetStats returns statistics for a subscription
func (m *SubscriptionManager) GetStats(ctx context.Context, id string) (*SubscriptionStats, error) {
	m.mu.RLock()
	subscriber, ok := m.subscribers[id]
	m.mu.RUnlock()

	if !ok {
		// Return stats from DB for stopped subscription
		sub, err := m.repo.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if sub == nil {
			return nil, fmt.Errorf("subscription not found: %s", id)
		}

		return &SubscriptionStats{
			ID:     sub.ID,
			Name:   sub.Name,
			Status: string(sub.Status),
		}, nil
	}

	return subscriber.GetStats(), nil
}

// GetAllStats returns statistics for all subscriptions
func (m *SubscriptionManager) GetAllStats(ctx context.Context) ([]*SubscriptionStats, error) {
	subscriptions, err := m.repo.List(ctx)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make([]*SubscriptionStats, 0, len(subscriptions))
	for _, sub := range subscriptions {
		if subscriber, ok := m.subscribers[sub.ID]; ok {
			stats = append(stats, subscriber.GetStats())
		} else {
			stats = append(stats, &SubscriptionStats{
				ID:     sub.ID,
				Name:   sub.Name,
				Status: string(sub.Status),
			})
		}
	}

	return stats, nil
}

// startSubscriber creates and starts a subscriber
func (m *SubscriptionManager) startSubscriber(sub *Subscription) error {
	subscriber := NewSubscriber(
		sub,
		m.arrowBuffer,
		m.encryptor,
		m.onStatusChange,
		m.logger,
	)

	if err := subscriber.Start(); err != nil {
		return err
	}

	m.mu.Lock()
	m.subscribers[sub.ID] = subscriber
	m.mu.Unlock()

	m.logger.Info().
		Str("id", sub.ID).
		Str("name", sub.Name).
		Msg("Started MQTT subscription")

	return nil
}

// onStatusChange handles status changes from subscribers
func (m *SubscriptionManager) onStatusChange(id string, status SubscriptionStatus, errMsg string) {
	ctx := context.Background()
	if err := m.repo.UpdateStatus(ctx, id, status, errMsg); err != nil {
		m.logger.Error().
			Err(err).
			Str("id", id).
			Str("status", string(status)).
			Msg("Failed to update subscription status")
	}
}
