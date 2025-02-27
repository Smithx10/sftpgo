// Package notifier defines the implementation for event notifier plugins.
// Notifier plugins allow to receive notifications for supported filesystem
// events such as file uploads, downloads etc. and user events such as add,
// update, delete.
package notifier

import (
	"context"
	"time"

	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	"github.com/drakkan/sftpgo/v2/sdk/plugin/notifier/proto"
)

const (
	// PluginName defines the name for a notifier plugin
	PluginName = "notifier"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "SFTPGO_NOTIFIER_PLUGIN",
	MagicCookieValue: "c499b98b-cd59-4df2-92b3-6268817f4d80",
}

// PluginMap is the map of plugins we can dispense.
var PluginMap = map[string]plugin.Plugin{
	PluginName: &Plugin{},
}

// Notifier defines the interface for notifiers plugins
type Notifier interface {
	NotifyFsEvent(timestamp time.Time, action, username, fsPath, fsTargetPath, sshCmd, protocol string,
		fileSize int64, status int) error
	NotifyUserEvent(timestamp time.Time, action string, user []byte) error
}

// Plugin defines the implementation to serve/connect to a notifier plugin
type Plugin struct {
	plugin.Plugin
	Impl Notifier
}

// GRPCServer defines the GRPC server implementation for this plugin
func (p *Plugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterNotifierServer(s, &GRPCServer{
		Impl: p.Impl,
	})
	return nil
}

// GRPCClient defines the GRPC client implementation for this plugin
func (p *Plugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client: proto.NewNotifierClient(c),
	}, nil
}
