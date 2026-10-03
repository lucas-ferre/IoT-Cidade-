package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	mathrand "math/rand"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	smartcitypb "github.com/lucas-ferre/projeto_socket/projeto-sockets/sensor_go/proto"
	"google.golang.org/protobuf/proto"
)

const (
	controlTCPPort       = 5007
	gatewayTelemetryPort = 5000
	gatewayDiscoveryPort = 5002
	multicastGroup       = "239.0.0.1"
	multicastPort        = 5005
	discoveryProbe       = "SMARTCITY_DISCOVERY_PROBE"

	defaultFrequencySecs = int32(5)
	minFrequencySecs     = int32(1)
	maxFrequencySecs     = int32(60)
	maxFrameBytes        = uint32(1024 * 1024)
	maxControlClients    = 32
	maxCommandAge        = 5 * time.Minute
	maxCommandFutureSkew = time.Minute
	commandReplayWindow  = 10 * time.Minute
	udpSendAttempts      = 3
)

var (
	commandIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	deviceIDPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,127}$`)
)

type configuration struct {
	gatewayHost       string
	advertisedHost    string
	deviceCount       int
	heartbeatInterval time.Duration
	heartbeatJitter   time.Duration
	discoveryJitter   time.Duration
}

type sectorTemplate struct {
	name     string
	slug     string
	capacity int
}

var sectorTemplates = []sectorTemplate{
	{name: "Centro", slug: "centro", capacity: 120},
	{name: "Campus", slug: "campus", capacity: 80},
	{name: "Hospital", slug: "hospital", capacity: 150},
}

type lockedRandom struct {
	mu  sync.Mutex
	rng *mathrand.Rand
}

func newLockedRandom() *lockedRandom {
	return &lockedRandom{rng: mathrand.New(mathrand.NewSource(time.Now().UnixNano()))}
}

func (r *lockedRandom) intn(limit int) int {
	if limit <= 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rng.Intn(limit)
}

func (r *lockedRandom) float64() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rng.Float64()
}

func (r *lockedRandom) duration(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return time.Duration(r.rng.Int63n(int64(limit) + 1))
}

type parkingDevice struct {
	id              string
	sector          string
	status          smartcitypb.DeviceStatus
	frequencySecs   int32
	nextSendAt      time.Time
	totalSpaces     int
	occupiedSpaces  int
	vehicleTurnover float64
}

type deviceSnapshot struct {
	id              string
	sector          string
	status          smartcitypb.DeviceStatus
	frequencySecs   int32
	totalSpaces     int
	occupiedSpaces  int
	vehicleTurnover float64
}

type fleet struct {
	mu             sync.RWMutex
	devices        map[string]*parkingDevice
	order          []string
	recentCommands map[string]time.Time
	random         *lockedRandom
}

func newFleet(deviceCount int, random *lockedRandom) *fleet {
	now := time.Now()
	f := &fleet{
		devices:        make(map[string]*parkingDevice, deviceCount),
		order:          make([]string, 0, deviceCount),
		recentCommands: make(map[string]time.Time),
		random:         random,
	}

	for index := 0; index < deviceCount; index++ {
		template := sectorTemplates[index%len(sectorTemplates)]
		ordinal := (index / len(sectorTemplates)) + 1
		deviceID := fmt.Sprintf("parking_%s_%02d", template.slug, ordinal)
		capacity := template.capacity + ((ordinal - 1) * 10)
		occupied := int(math.Round(float64(capacity) * (0.35 + random.float64()*0.35)))

		f.devices[deviceID] = &parkingDevice{
			id:              deviceID,
			sector:          template.name,
			status:          smartcitypb.DeviceStatus_STATUS_ON,
			frequencySecs:   defaultFrequencySecs,
			nextSendAt:      now,
			totalSpaces:     capacity,
			occupiedSpaces:  occupied,
			vehicleTurnover: 0.2 + random.float64()*1.3,
		}
		f.order = append(f.order, deviceID)
	}

	return f
}

func snapshotOf(device *parkingDevice) deviceSnapshot {
	return deviceSnapshot{
		id:              device.id,
		sector:          device.sector,
		status:          device.status,
		frequencySecs:   device.frequencySecs,
		totalSpaces:     device.totalSpaces,
		occupiedSpaces:  device.occupiedSpaces,
		vehicleTurnover: device.vehicleTurnover,
	}
}

func (f *fleet) snapshots(targetDeviceID string) []deviceSnapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if targetDeviceID != "" {
		device, exists := f.devices[targetDeviceID]
		if !exists {
			return nil
		}
		return []deviceSnapshot{snapshotOf(device)}
	}

	result := make([]deviceSnapshot, 0, len(f.order))
	for _, deviceID := range f.order {
		result = append(result, snapshotOf(f.devices[deviceID]))
	}
	return result
}

func (f *fleet) evolve(device *parkingDevice) {
	occupancyRatio := float64(device.occupiedSpaces) / float64(device.totalSpaces)
	draw := f.random.float64()
	delta := 0

	switch {
	case draw < 0.38:
		delta = 0
	case occupancyRatio >= 0.90:
		delta = -(1 + f.random.intn(2))
	case occupancyRatio <= 0.15:
		delta = 1 + f.random.intn(2)
	case f.random.intn(2) == 0:
		delta = -(1 + f.random.intn(2))
	default:
		delta = 1 + f.random.intn(2)
	}

	device.occupiedSpaces = clamp(device.occupiedSpaces+delta, 0, device.totalSpaces)
	// A rotatividade expressa uma taxa estimada e suavizada, não apenas a
	// diferença líquida de ocupação (uma chegada e uma saída podem se anular).
	targetTurnover := 0.2 + f.random.float64()*2.3
	device.vehicleTurnover = (device.vehicleTurnover * 0.65) + (targetTurnover * 0.35)
}

func (f *fleet) dueSnapshots(now time.Time) []deviceSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]deviceSnapshot, 0, len(f.order))
	for _, deviceID := range f.order {
		device := f.devices[deviceID]
		if now.Before(device.nextSendAt) {
			continue
		}

		if device.status == smartcitypb.DeviceStatus_STATUS_ON {
			f.evolve(device)
		} else {
			device.vehicleTurnover = 0
		}

		frequency := time.Duration(device.frequencySecs) * time.Second
		device.nextSendAt = now.Add(frequency + f.random.duration(350*time.Millisecond))
		result = append(result, snapshotOf(device))
	}
	return result
}

func (f *fleet) applyCommand(command *smartcitypb.ConfigCommand, now time.Time) (deviceSnapshot, error) {
	if err := validateCommand(command, now); err != nil {
		return deviceSnapshot{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	device, exists := f.devices[command.GetTargetDeviceId()]
	if !exists {
		return deviceSnapshot{}, fmt.Errorf("dispositivo alvo desconhecido: %s", command.GetTargetDeviceId())
	}

	for commandID, processedAt := range f.recentCommands {
		if now.Sub(processedAt) > commandReplayWindow {
			delete(f.recentCommands, commandID)
		}
	}
	if _, duplicate := f.recentCommands[command.GetCommandId()]; duplicate {
		return snapshotOf(device), fmt.Errorf("command_id já processado: %s", command.GetCommandId())
	}

	if command.GetUpdateStatus() {
		device.status = command.GetTargetStatus()
	}
	if command.GetUpdateFrequency() {
		device.frequencySecs = command.GetNewFrequencySecs()
	}
	device.nextSendAt = now
	f.recentCommands[command.GetCommandId()] = now
	return snapshotOf(device), nil
}

func (f *fleet) markAllOff() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, device := range f.devices {
		device.status = smartcitypb.DeviceStatus_STATUS_OFF
		device.vehicleTurnover = 0
	}
}

func validateCommand(command *smartcitypb.ConfigCommand, now time.Time) error {
	if command == nil {
		return errors.New("comando ausente")
	}
	if !commandIDPattern.MatchString(command.GetCommandId()) {
		return errors.New("command_id ausente ou com formato inválido")
	}
	if !deviceIDPattern.MatchString(command.GetTargetDeviceId()) {
		return errors.New("target_device_id ausente ou com formato inválido")
	}
	if command.GetTimestamp() <= 0 {
		return errors.New("timestamp ausente ou inválido")
	}

	commandTime := time.Unix(command.GetTimestamp(), 0)
	if commandTime.Before(now.Add(-maxCommandAge)) {
		return fmt.Errorf("timestamp expirado: idade máxima de %s", maxCommandAge)
	}
	if commandTime.After(now.Add(maxCommandFutureSkew)) {
		return fmt.Errorf("timestamp futuro além da tolerância de %s", maxCommandFutureSkew)
	}
	if !command.GetUpdateStatus() && !command.GetUpdateFrequency() {
		return errors.New("comando não solicita alteração de status nem de frequência")
	}
	if command.GetUpdateStatus() {
		switch command.GetTargetStatus() {
		case smartcitypb.DeviceStatus_STATUS_ON, smartcitypb.DeviceStatus_STATUS_OFF:
		default:
			return errors.New("target_status deve ser STATUS_ON ou STATUS_OFF")
		}
	}
	if command.GetUpdateFrequency() {
		frequency := command.GetNewFrequencySecs()
		if frequency < minFrequencySecs || frequency > maxFrequencySecs {
			return fmt.Errorf("new_frequency_secs deve estar entre %d e %d", minFrequencySecs, maxFrequencySecs)
		}
	}
	return nil
}

type sensor struct {
	config       configuration
	fleet        *fleet
	random       *lockedRandom
	logger       *log.Logger
	probes       chan struct{}
	controlReady chan struct{}
}

func newSensor(config configuration, logger *log.Logger) *sensor {
	random := newLockedRandom()
	return &sensor{
		config:       config,
		fleet:        newFleet(config.deviceCount, random),
		random:       random,
		logger:       logger,
		probes:       make(chan struct{}, 16),
		controlReady: make(chan struct{}),
	}
}

func (s *sensor) run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var workers sync.WaitGroup
	fatalErrors := make(chan error, 2)
	start := func(name string, worker func(context.Context) error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := worker(ctx); err != nil && ctx.Err() == nil {
				select {
				case fatalErrors <- fmt.Errorf("%s: %w", name, err):
				default:
				}
			}
		}()
	}

	start("servidor TCP", s.controlServer)
	start("listener multicast", s.multicastListener)
	start("dispatcher de probes", func(ctx context.Context) error {
		s.probeDispatcher(ctx)
		return nil
	})
	start("heartbeat", func(ctx context.Context) error {
		s.heartbeatLoop(ctx)
		return nil
	})
	start("telemetria", func(ctx context.Context) error {
		s.telemetryLoop(ctx)
		return nil
	})

	// Não anuncie uma porta de controle antes de o listener realmente aceitá-la.
	select {
	case <-s.controlReady:
	case runError := <-fatalErrors:
		cancel()
		workers.Wait()
		return runError
	case <-parent.Done():
		cancel()
		workers.Wait()
		return nil
	}

	s.logger.Printf("frota iniciada com %d setores; controle TCP na porta %d", s.config.deviceCount, controlTCPPort)
	for _, device := range s.fleet.snapshots("") {
		s.logger.Printf("dispositivo=%s setor=%s capacidade=%d", device.id, device.sector, device.totalSpaces)
	}
	if err := s.sendDiscovery(ctx, ""); err != nil && ctx.Err() == nil {
		s.logger.Printf("descoberta inicial incompleta: %v", err)
	}

	var runError error
	select {
	case <-parent.Done():
	case runError = <-fatalErrors:
	}
	cancel()
	workers.Wait()

	// O último anúncio informa STATUS_OFF ao gateway. Um contexto próprio e
	// curto evita que a tentativa de despedida bloqueie o encerramento.
	s.fleet.markAllOff()
	shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	if err := s.sendDiscovery(shutdownContext, ""); err != nil {
		s.logger.Printf("não foi possível concluir o anúncio de desligamento: %v", err)
	}
	shutdownCancel()
	s.logger.Print("encerramento gracioso concluído")
	return runError
}

func (s *sensor) telemetryLoop(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, snapshot := range s.fleet.dueSnapshots(now) {
				s.sendTelemetry(ctx, snapshot)
			}
		}
	}
}

func (s *sensor) sendTelemetry(ctx context.Context, snapshot deviceSnapshot) {
	payload := &smartcitypb.DataPayload{
		MessageId:     newMessageID(snapshot.id),
		Timestamp:     time.Now().Unix(),
		DeviceId:      snapshot.id,
		CurrentStatus: snapshot.status,
		Metrics:       parkingMetrics(snapshot),
	}

	if err := s.sendUDP(ctx, payload, gatewayTelemetryPort); err != nil && ctx.Err() == nil {
		s.logger.Printf("falha de telemetria dispositivo=%s: %v", snapshot.id, err)
		return
	}
	if snapshot.status == smartcitypb.DeviceStatus_STATUS_ON {
		s.logger.Printf(
			"telemetria dispositivo=%s ocupadas=%d/%d rotatividade=%.2f veículos/min",
			snapshot.id,
			snapshot.occupiedSpaces,
			snapshot.totalSpaces,
			snapshot.vehicleTurnover,
		)
	} else {
		s.logger.Printf("telemetria dispositivo=%s status=%s", snapshot.id, snapshot.status.String())
	}
}

func parkingMetrics(snapshot deviceSnapshot) []*smartcitypb.Metric {
	if snapshot.status != smartcitypb.DeviceStatus_STATUS_ON || snapshot.totalSpaces <= 0 {
		return nil
	}
	available := snapshot.totalSpaces - snapshot.occupiedSpaces
	occupancyRate := 100 * float64(snapshot.occupiedSpaces) / float64(snapshot.totalSpaces)
	return []*smartcitypb.Metric{
		{Name: "total_spaces", Value: float64(snapshot.totalSpaces), Unit: "spaces"},
		{Name: "occupied_spaces", Value: float64(snapshot.occupiedSpaces), Unit: "spaces"},
		{Name: "available_spaces", Value: float64(available), Unit: "spaces"},
		{Name: "occupancy_rate", Value: occupancyRate, Unit: "%"},
		{Name: "vehicle_turnover", Value: snapshot.vehicleTurnover, Unit: "vehicles/min"},
	}
}

func (s *sensor) heartbeatLoop(ctx context.Context) {
	for {
		delay := s.config.heartbeatInterval + s.random.duration(s.config.heartbeatJitter)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C:
			if err := s.sendDiscovery(ctx, ""); err != nil && ctx.Err() == nil {
				s.logger.Printf("heartbeat de descoberta incompleto: %v", err)
			}
		}
	}
}

func (s *sensor) sendDiscovery(ctx context.Context, targetDeviceID string) error {
	devices := s.fleet.snapshots(targetDeviceID)
	if len(devices) == 0 {
		return fmt.Errorf("nenhum dispositivo encontrado para %q", targetDeviceID)
	}

	var sendErrors []error
	for _, device := range devices {
		response := &smartcitypb.DiscoveryResponse{
			MessageId:      newMessageID("DISC"),
			Timestamp:      time.Now().Unix(),
			DeviceId:       device.id,
			Type:           smartcitypb.DeviceType_DEVICE_TYPE_PARKING_SENSOR,
			IpAddress:      s.config.advertisedHost,
			ControlPort:    controlTCPPort,
			InitialStatus:  device.status,
			IsControllable: true,
		}
		if err := s.sendUDP(ctx, response, gatewayDiscoveryPort); err != nil {
			sendErrors = append(sendErrors, fmt.Errorf("%s: %w", device.id, err))
		}
	}
	return errors.Join(sendErrors...)
}

func (s *sensor) sendUDP(ctx context.Context, message proto.Message, port int) error {
	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("serializar Protobuf: %w", err)
	}

	address := net.JoinHostPort(s.config.gatewayHost, strconv.Itoa(port))
	var lastError error
	for attempt := 0; attempt < udpSendAttempts; attempt++ {
		dialer := net.Dialer{Timeout: time.Second}
		connection, dialError := dialer.DialContext(ctx, "udp4", address)
		if dialError == nil {
			_ = connection.SetWriteDeadline(time.Now().Add(time.Second))
			_, lastError = connection.Write(payload)
			closeError := connection.Close()
			if lastError == nil {
				lastError = closeError
			}
			if lastError == nil {
				return nil
			}
		} else {
			lastError = dialError
		}

		if attempt == udpSendAttempts-1 {
			break
		}
		backoff := (200 * time.Millisecond * time.Duration(1<<attempt)) + s.random.duration(150*time.Millisecond)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("envio UDP para %s falhou após %d tentativas: %w", address, udpSendAttempts, lastError)
}

func (s *sensor) multicastListener(ctx context.Context) error {
	groupAddress := &net.UDPAddr{IP: net.ParseIP(multicastGroup), Port: multicastPort}
	connection, err := net.ListenMulticastUDP("udp4", nil, groupAddress)
	if err != nil {
		return fmt.Errorf("assinar %s:%d: %w", multicastGroup, multicastPort, err)
	}
	defer connection.Close()
	_ = connection.SetReadBuffer(64 * 1024)
	s.logger.Printf("aguardando probes multicast em %s:%d", multicastGroup, multicastPort)

	buffer := make([]byte, 256)
	for {
		if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		length, peer, readError := connection.ReadFromUDP(buffer)
		if readError != nil {
			if ctx.Err() != nil {
				return nil
			}
			if netError, ok := readError.(net.Error); ok && netError.Timeout() {
				continue
			}
			return fmt.Errorf("ler probe: %w", readError)
		}
		if !bytes.Equal(buffer[:length], []byte(discoveryProbe)) {
			continue
		}

		select {
		case s.probes <- struct{}{}:
			s.logger.Printf("probe multicast recebido de %s", peer.IP)
		default:
			s.logger.Print("fila de probes cheia; probe redundante descartado")
		}
	}
}

func (s *sensor) probeDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.probes:
		}

		jitter := s.random.duration(s.config.discoveryJitter)
		timer := time.NewTimer(jitter)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C:
		}

		coalesced := 0
	drain:
		for {
			select {
			case <-s.probes:
				coalesced++
			default:
				break drain
			}
		}
		if coalesced > 0 {
			s.logger.Printf("%d probes redundantes coalescidos", coalesced)
		}
		if err := s.sendDiscovery(ctx, ""); err != nil && ctx.Err() == nil {
			s.logger.Printf("resposta ao probe incompleta: %v", err)
		}
	}
}

func (s *sensor) controlServer(ctx context.Context) error {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{Port: controlTCPPort})
	if err != nil {
		return fmt.Errorf("escutar porta %d: %w", controlTCPPort, err)
	}
	defer listener.Close()
	close(s.controlReady)

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	semaphore := make(chan struct{}, maxControlClients)
	var handlers sync.WaitGroup
	for {
		connection, acceptError := listener.AcceptTCP()
		if acceptError != nil {
			if ctx.Err() != nil || errors.Is(acceptError, net.ErrClosed) {
				break
			}
			if netError, ok := acceptError.(net.Error); ok && netError.Temporary() {
				s.logger.Printf("falha temporária no accept TCP: %v", acceptError)
				continue
			}
			return fmt.Errorf("accept TCP: %w", acceptError)
		}

		select {
		case semaphore <- struct{}{}:
			handlers.Add(1)
			go func(connection *net.TCPConn) {
				defer handlers.Done()
				defer func() { <-semaphore }()
				s.handleControlConnection(ctx, connection)
			}(connection)
		default:
			s.logger.Printf("limite de %d clientes TCP atingido; conexão rejeitada", maxControlClients)
			_ = connection.Close()
		}
	}

	handlers.Wait()
	return nil
}

func (s *sensor) handleControlConnection(ctx context.Context, connection *net.TCPConn) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(4 * time.Second))

	frame, err := readFrame(connection)
	if err != nil {
		// O healthcheck abre e fecha uma conexão deliberadamente, sem frame.
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			s.logger.Printf("frame TCP recusado de %s: %v", connection.RemoteAddr(), err)
		}
		return
	}

	command := &smartcitypb.ConfigCommand{}
	if err := proto.Unmarshal(frame, command); err != nil {
		s.writeControlError(connection, "", fmt.Errorf("Protobuf inválido: %w", err), deviceSnapshot{})
		return
	}

	snapshot, commandError := s.fleet.applyCommand(command, time.Now())
	if commandError != nil {
		s.writeControlError(connection, command.GetCommandId(), commandError, snapshot)
		return
	}

	response := &smartcitypb.ConfigResponse{
		MessageId:            newMessageID("ACK"),
		CommandId:            command.GetCommandId(),
		Timestamp:            time.Now().Unix(),
		Success:              true,
		Message:              fmt.Sprintf("Estacionamento %s reconfigurado com sucesso.", snapshot.id),
		UpdatedStatus:        snapshot.status,
		UpdatedFrequencySecs: snapshot.frequencySecs,
	}
	if err := writeProtoFrame(connection, response); err != nil {
		s.logger.Printf("falha ao responder comando=%s: %v", command.GetCommandId(), err)
		return
	}
	s.logger.Printf(
		"comando=%s dispositivo=%s status=%s frequência=%ds aplicado",
		command.GetCommandId(),
		snapshot.id,
		snapshot.status.String(),
		snapshot.frequencySecs,
	)

	if err := s.sendDiscovery(ctx, snapshot.id); err != nil && ctx.Err() == nil {
		s.logger.Printf("não foi possível atualizar descoberta de %s: %v", snapshot.id, err)
	}
}

func (s *sensor) writeControlError(
	connection net.Conn,
	commandID string,
	commandError error,
	snapshot deviceSnapshot,
) {
	response := &smartcitypb.ConfigResponse{
		MessageId:            newMessageID("ERR"),
		CommandId:            commandID,
		Timestamp:            time.Now().Unix(),
		Success:              false,
		Message:              "Comando rejeitado: " + commandError.Error(),
		UpdatedStatus:        snapshot.status,
		UpdatedFrequencySecs: snapshot.frequencySecs,
	}
	if err := writeProtoFrame(connection, response); err != nil {
		s.logger.Printf("falha ao enviar resposta negativa: %v", err)
	}
	s.logger.Printf("comando=%s rejeitado: %v", commandID, commandError)
}

func readFrame(reader io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}
	frameSize := binary.BigEndian.Uint32(header)
	if frameSize == 0 || frameSize > maxFrameBytes {
		return nil, fmt.Errorf("tamanho de frame inválido: %d", frameSize)
	}
	payload := make([]byte, frameSize)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writeProtoFrame(writer io.Writer, message proto.Message) error {
	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("serializar resposta: %w", err)
	}
	if len(payload) == 0 || len(payload) > int(maxFrameBytes) {
		return fmt.Errorf("tamanho de resposta inválido: %d", len(payload))
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if err := writeAll(writer, header); err != nil {
		return err
	}
	if err := writeAll(writer, payload); err != nil {
		return err
	}
	return nil
}

func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func loadConfiguration() (configuration, error) {
	deviceCount, err := boundedEnvInt("GO_PARKING_DEVICE_COUNT", 3, 1, 100)
	if err != nil {
		return configuration{}, err
	}
	heartbeatInterval, err := boundedEnvSeconds("SENSOR_HEARTBEAT_INTERVAL_SECS", 10, 1, 3600)
	if err != nil {
		return configuration{}, err
	}
	heartbeatJitter, err := boundedEnvSeconds("SENSOR_HEARTBEAT_JITTER_SECS", 2, 0, 60)
	if err != nil {
		return configuration{}, err
	}
	discoveryJitter, err := boundedEnvSeconds("SENSOR_DISCOVERY_JITTER_SECS", 2, 0, 30)
	if err != nil {
		return configuration{}, err
	}

	gatewayHost := strings.TrimSpace(envOrDefault("GATEWAY_HOST", "gateway"))
	if gatewayHost == "" {
		return configuration{}, errors.New("GATEWAY_HOST não pode ser vazio")
	}
	advertisedHost := strings.TrimSpace(envOrDefault("SENSOR_HOSTNAME", hostnameOrDefault("sensor_estacionamento")))
	if advertisedHost == "" {
		return configuration{}, errors.New("SENSOR_HOSTNAME não pode ser vazio")
	}

	return configuration{
		gatewayHost:       gatewayHost,
		advertisedHost:    advertisedHost,
		deviceCount:       deviceCount,
		heartbeatInterval: heartbeatInterval,
		heartbeatJitter:   heartbeatJitter,
		discoveryJitter:   discoveryJitter,
	}, nil
}

func boundedEnvInt(name string, fallback, minimum, maximum int) (int, error) {
	raw := strings.TrimSpace(envOrDefault(name, strconv.Itoa(fallback)))
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s deve ser inteiro entre %d e %d; recebido %q", name, minimum, maximum, raw)
	}
	return value, nil
}

func boundedEnvSeconds(name string, fallback, minimum, maximum float64) (time.Duration, error) {
	raw := strings.TrimSpace(envOrDefault(name, strconv.FormatFloat(fallback, 'f', -1, 64)))
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s deve estar entre %.2f e %.2f segundos; recebido %q", name, minimum, maximum, raw)
	}
	return time.Duration(value * float64(time.Second)), nil
}

func runHealthcheck() error {
	address := envOrDefault("SENSOR_HEALTHCHECK_ADDRESS", net.JoinHostPort("127.0.0.1", strconv.Itoa(controlTCPPort)))
	connection, err := net.DialTimeout("tcp4", address, 1500*time.Millisecond)
	if err != nil {
		return fmt.Errorf("controle TCP indisponível em %s: %w", address, err)
	}
	return connection.Close()
}

func envOrDefault(name, fallback string) string {
	if value, exists := os.LookupEnv(name); exists {
		return value
	}
	return fallback
}

func hostnameOrDefault(fallback string) string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return fallback
	}
	return hostname
}

func newMessageID(prefix string) string {
	randomBytes := make([]byte, 12)
	if _, err := cryptorand.Read(randomBytes); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%x", prefix, randomBytes)
}

func clamp(value, minimum, maximum int) int {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "healthcheck" || os.Args[1] == "--healthcheck") {
		if err := runHealthcheck(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 {
		fmt.Fprintf(os.Stderr, "uso: %s [healthcheck]\n", os.Args[0])
		os.Exit(2)
	}

	logger := log.New(os.Stdout, "[sensor_estacionamento] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.LUTC)
	config, err := loadConfiguration()
	if err != nil {
		logger.Fatalf("configuração inválida: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newSensor(config, logger).run(ctx); err != nil {
		logger.Printf("sensor encerrado por falha: %v", err)
		os.Exit(1)
	}
}
