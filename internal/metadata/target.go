package metadata

type NotificationTarget struct {
	ID         string `json:"id"`       // unique per user
	OwnerID    string `json:"owner_id"` // auth subject
	Type       string `json:"type"`     // "rabbitmq"
	URL        string `json:"url"`      // amqp://...
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key,omitempty"`
	Durable    bool   `json:"durable"`
	Enabled    bool   `json:"enabled"`
}
