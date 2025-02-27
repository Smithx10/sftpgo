package plugin

import (
	"crypto/sha256"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/drakkan/sftpgo/v2/logger"
	"github.com/drakkan/sftpgo/v2/sdk/plugin/notifier"
	"github.com/drakkan/sftpgo/v2/sdk/plugin/notifier/proto"
	"github.com/drakkan/sftpgo/v2/util"
)

// NotifierConfig defines configuration parameters for notifiers plugins
type NotifierConfig struct {
	FsEvents          []string `json:"fs_events" mapstructure:"fs_events"`
	UserEvents        []string `json:"user_events" mapstructure:"user_events"`
	RetryMaxTime      int      `json:"retry_max_time" mapstructure:"retry_max_time"`
	RetryQueueMaxSize int      `json:"retry_queue_max_size" mapstructure:"retry_queue_max_size"`
}

func (c *NotifierConfig) hasActions() bool {
	if len(c.FsEvents) > 0 {
		return true
	}
	if len(c.UserEvents) > 0 {
		return true
	}
	return false
}

type eventsQueue struct {
	sync.RWMutex
	fsEvents   []*proto.FsEvent
	userEvents []*proto.UserEvent
}

func (q *eventsQueue) addFsEvent(timestamp time.Time, action, username, fsPath, fsTargetPath, sshCmd, protocol string, fileSize int64, status int) {
	q.Lock()
	defer q.Unlock()

	q.fsEvents = append(q.fsEvents, &proto.FsEvent{
		Timestamp:    timestamppb.New(timestamp),
		Action:       action,
		Username:     username,
		FsPath:       fsPath,
		FsTargetPath: fsTargetPath,
		SshCmd:       sshCmd,
		FileSize:     fileSize,
		Protocol:     protocol,
		Status:       int32(status),
	})
}

func (q *eventsQueue) addUserEvent(timestamp time.Time, action string, userAsJSON []byte) {
	q.Lock()
	defer q.Unlock()

	q.userEvents = append(q.userEvents, &proto.UserEvent{
		Timestamp: timestamppb.New(timestamp),
		Action:    action,
		User:      userAsJSON,
	})
}

func (q *eventsQueue) popFsEvent() *proto.FsEvent {
	q.Lock()
	defer q.Unlock()

	if len(q.fsEvents) == 0 {
		return nil
	}
	truncLen := len(q.fsEvents) - 1
	ev := q.fsEvents[truncLen]
	q.fsEvents[truncLen] = nil
	q.fsEvents = q.fsEvents[:truncLen]

	return ev
}

func (q *eventsQueue) popUserEvent() *proto.UserEvent {
	q.Lock()
	defer q.Unlock()

	if len(q.userEvents) == 0 {
		return nil
	}
	truncLen := len(q.userEvents) - 1
	ev := q.userEvents[truncLen]
	q.userEvents[truncLen] = nil
	q.userEvents = q.userEvents[:truncLen]

	return ev
}

func (q *eventsQueue) getSize() int {
	q.RLock()
	defer q.RUnlock()

	return len(q.userEvents) + len(q.fsEvents)
}

type notifierPlugin struct {
	config   Config
	notifier notifier.Notifier
	client   *plugin.Client
	queue    *eventsQueue
}

func newNotifierPlugin(config Config) (*notifierPlugin, error) {
	p := &notifierPlugin{
		config: config,
		queue:  &eventsQueue{},
	}
	if err := p.initialize(); err != nil {
		logger.Warn(logSender, "", "unable to create notifier plugin: %v, config %+v", err, config)
		return nil, err
	}
	return p, nil
}

func (p *notifierPlugin) exited() bool {
	return p.client.Exited()
}

func (p *notifierPlugin) cleanup() {
	p.client.Kill()
}

func (p *notifierPlugin) initialize() error {
	killProcess(p.config.Cmd)
	logger.Debug(logSender, "", "create new notifier plugin %#v", p.config.Cmd)
	if !p.config.NotifierOptions.hasActions() {
		return fmt.Errorf("no actions defined for the notifier plugin %#v", p.config.Cmd)
	}
	var secureConfig *plugin.SecureConfig
	if p.config.SHA256Sum != "" {
		secureConfig.Checksum = []byte(p.config.SHA256Sum)
		secureConfig.Hash = sha256.New()
	}
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: notifier.Handshake,
		Plugins:         notifier.PluginMap,
		Cmd:             exec.Command(p.config.Cmd, p.config.Args...),
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolGRPC,
		},
		AutoMTLS:     p.config.AutoMTLS,
		SecureConfig: secureConfig,
		Managed:      false,
		Logger: &logger.HCLogAdapter{
			Logger: hclog.New(&hclog.LoggerOptions{
				Name:        fmt.Sprintf("%v.%v", logSender, notifier.PluginName),
				Level:       pluginsLogLevel,
				DisableTime: true,
			}),
		},
	})
	rpcClient, err := client.Client()
	if err != nil {
		logger.Debug(logSender, "", "unable to get rpc client for plugin %#v: %v", p.config.Cmd, err)
		return err
	}
	raw, err := rpcClient.Dispense(notifier.PluginName)
	if err != nil {
		logger.Debug(logSender, "", "unable to get plugin %v from rpc client for command %#v: %v",
			notifier.PluginName, p.config.Cmd, err)
		return err
	}

	p.client = client
	p.notifier = raw.(notifier.Notifier)

	return nil
}

func (p *notifierPlugin) canQueueEvent(timestamp time.Time) bool {
	if p.config.NotifierOptions.RetryMaxTime == 0 {
		return false
	}
	if time.Now().After(timestamp.Add(time.Duration(p.config.NotifierOptions.RetryMaxTime) * time.Second)) {
		return false
	}
	if p.config.NotifierOptions.RetryQueueMaxSize > 0 {
		return p.queue.getSize() < p.config.NotifierOptions.RetryQueueMaxSize
	}
	return true
}

func (p *notifierPlugin) notifyFsAction(timestamp time.Time, action, username, fsPath, fsTargetPath, sshCmd,
	protocol string, fileSize int64, errAction error) {
	if !util.IsStringInSlice(action, p.config.NotifierOptions.FsEvents) {
		return
	}

	go func() {
		status := 1
		if errAction != nil {
			status = 0
		}
		p.sendFsEvent(timestamp, action, username, fsPath, fsTargetPath, sshCmd, protocol, fileSize, status)
	}()
}

func (p *notifierPlugin) notifyUserAction(timestamp time.Time, action string, user Renderer) {
	if !util.IsStringInSlice(action, p.config.NotifierOptions.UserEvents) {
		return
	}

	go func() {
		userAsJSON, err := user.RenderAsJSON(action != "delete")
		if err != nil {
			logger.Warn(logSender, "", "unable to render user as json for action %v: %v", action, err)
			return
		}
		p.sendUserEvent(timestamp, action, userAsJSON)
	}()
}

func (p *notifierPlugin) sendFsEvent(timestamp time.Time, action, username, fsPath, fsTargetPath, sshCmd,
	protocol string, fileSize int64, status int) {
	if err := p.notifier.NotifyFsEvent(timestamp, action, username, fsPath, fsTargetPath, sshCmd, protocol, fileSize, status); err != nil {
		logger.Warn(logSender, "", "unable to send fs action notification to plugin %v: %v", p.config.Cmd, err)
		if p.canQueueEvent(timestamp) {
			p.queue.addFsEvent(timestamp, action, username, fsPath, fsTargetPath, sshCmd, protocol, fileSize, status)
		}
	}
}

func (p *notifierPlugin) sendUserEvent(timestamp time.Time, action string, userAsJSON []byte) {
	if err := p.notifier.NotifyUserEvent(timestamp, action, userAsJSON); err != nil {
		logger.Warn(logSender, "", "unable to send user action notification to plugin %v: %v", p.config.Cmd, err)
		if p.canQueueEvent(timestamp) {
			p.queue.addUserEvent(timestamp, action, userAsJSON)
		}
	}
}

func (p *notifierPlugin) sendQueuedEvents() {
	queueSize := p.queue.getSize()
	if queueSize == 0 {
		return
	}
	logger.Debug(logSender, "", "check queued events for notifier %#v, events size: %v", p.config.Cmd, queueSize)
	fsEv := p.queue.popFsEvent()
	for fsEv != nil {
		go p.sendFsEvent(fsEv.Timestamp.AsTime(), fsEv.Action, fsEv.Username, fsEv.FsPath, fsEv.FsTargetPath,
			fsEv.SshCmd, fsEv.Protocol, fsEv.FileSize, int(fsEv.Status))
		fsEv = p.queue.popFsEvent()
	}

	userEv := p.queue.popUserEvent()
	for userEv != nil {
		go p.sendUserEvent(userEv.Timestamp.AsTime(), userEv.Action, userEv.User)
		userEv = p.queue.popUserEvent()
	}
	logger.Debug(logSender, "", "queued events sent for notifier %#v, new events size: %v", p.config.Cmd, p.queue.getSize())
}
