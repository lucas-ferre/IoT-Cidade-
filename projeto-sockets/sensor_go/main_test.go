package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	smartcitypb "github.com/lucas-ferre/projeto_socket/projeto-sockets/sensor_go/proto"
	"google.golang.org/protobuf/proto"
)

func validTestCommand(now time.Time) *smartcitypb.ConfigCommand {
	return &smartcitypb.ConfigCommand{
		CommandId:        "CMD-TEST-001",
		Timestamp:        now.Unix(),
		UpdateStatus:     true,
		TargetStatus:     smartcitypb.DeviceStatus_STATUS_OFF,
		UpdateFrequency:  true,
		NewFrequencySecs: 15,
		TargetDeviceId:   "parking_centro_01",
	}
}

func TestValidateCommand(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name    string
		mutate  func(*smartcitypb.ConfigCommand)
		wantErr bool
	}{
		{name: "válido", mutate: func(*smartcitypb.ConfigCommand) {}, wantErr: false},
		{name: "sem command id", mutate: func(command *smartcitypb.ConfigCommand) { command.CommandId = "" }, wantErr: true},
		{name: "timestamp expirado", mutate: func(command *smartcitypb.ConfigCommand) { command.Timestamp = now.Add(-6 * time.Minute).Unix() }, wantErr: true},
		{name: "destino inválido", mutate: func(command *smartcitypb.ConfigCommand) { command.TargetDeviceId = "PARKING/01" }, wantErr: true},
		{name: "status error não atuável", mutate: func(command *smartcitypb.ConfigCommand) { command.TargetStatus = smartcitypb.DeviceStatus_STATUS_ERROR }, wantErr: true},
		{name: "frequência abaixo do mínimo", mutate: func(command *smartcitypb.ConfigCommand) { command.NewFrequencySecs = 0 }, wantErr: true},
		{name: "frequência acima do máximo", mutate: func(command *smartcitypb.ConfigCommand) { command.NewFrequencySecs = 61 }, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := validTestCommand(now)
			test.mutate(command)
			err := validateCommand(command, now)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateCommand() erro=%v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestFleetAppliesCommandPerDeviceAndRejectsReplay(t *testing.T) {
	now := time.Now()
	f := newFleet(3, newLockedRandom())
	command := validTestCommand(now)

	snapshot, err := f.applyCommand(command, now)
	if err != nil {
		t.Fatalf("applyCommand() retornou erro: %v", err)
	}
	if snapshot.status != smartcitypb.DeviceStatus_STATUS_OFF {
		t.Fatalf("status=%s; esperado STATUS_OFF", snapshot.status)
	}
	if snapshot.frequencySecs != 15 {
		t.Fatalf("frequência=%d; esperado 15", snapshot.frequencySecs)
	}

	other := f.snapshots("parking_campus_01")
	if len(other) != 1 || other[0].status != smartcitypb.DeviceStatus_STATUS_ON {
		t.Fatalf("comando alterou dispositivo não destinado: %+v", other)
	}
	if _, err := f.applyCommand(command, now); err == nil {
		t.Fatal("replay do mesmo command_id deveria ser recusado")
	}
}

func TestParkingMetricsRemainConsistent(t *testing.T) {
	s := newSensor(configuration{deviceCount: 3}, nil)
	for iteration := 0; iteration < 250; iteration++ {
		snapshots := s.fleet.dueSnapshots(time.Now().Add(time.Duration(iteration) * time.Minute))
		for _, snapshot := range snapshots {
			if snapshot.totalSpaces <= 0 {
				t.Fatalf("capacidade inválida: %d", snapshot.totalSpaces)
			}
			if snapshot.occupiedSpaces < 0 || snapshot.occupiedSpaces > snapshot.totalSpaces {
				t.Fatalf("ocupação fora dos limites: %d/%d", snapshot.occupiedSpaces, snapshot.totalSpaces)
			}
			if snapshot.vehicleTurnover < 0 {
				t.Fatalf("rotatividade negativa: %f", snapshot.vehicleTurnover)
			}

			metrics := parkingMetrics(snapshot)
			if len(metrics) != 5 {
				t.Fatalf("quantidade de métricas=%d; esperado 5", len(metrics))
			}
			if metrics[0].GetName() != "total_spaces" ||
				metrics[1].GetName() != "occupied_spaces" ||
				metrics[2].GetName() != "available_spaces" ||
				metrics[3].GetName() != "occupancy_rate" ||
				metrics[4].GetName() != "vehicle_turnover" {
				t.Fatalf("nomes de métricas inesperados: %v", metrics)
			}
			if metrics[1].GetValue()+metrics[2].GetValue() != metrics[0].GetValue() {
				t.Fatalf("ocupadas + disponíveis diverge do total: %v", metrics)
			}
		}
	}
}

func TestLengthPrefixedFrameRoundTrip(t *testing.T) {
	want := &smartcitypb.ConfigResponse{
		MessageId:            "ACK-TEST",
		CommandId:            "CMD-TEST",
		Timestamp:            123,
		Success:              true,
		UpdatedStatus:        smartcitypb.DeviceStatus_STATUS_ON,
		UpdatedFrequencySecs: 5,
	}

	var framed bytes.Buffer
	if err := writeProtoFrame(&framed, want); err != nil {
		t.Fatalf("writeProtoFrame() retornou erro: %v", err)
	}
	payload, err := readFrame(&framed)
	if err != nil {
		t.Fatalf("readFrame() retornou erro: %v", err)
	}
	got := &smartcitypb.ConfigResponse{}
	if err := proto.Unmarshal(payload, got); err != nil {
		t.Fatalf("proto.Unmarshal() retornou erro: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("resposta divergente: got=%v want=%v", got, want)
	}
}

func TestReadFrameRejectsOversize(t *testing.T) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, maxFrameBytes+1)
	if _, err := readFrame(bytes.NewReader(header)); err == nil {
		t.Fatal("frame acima do limite deveria ser recusado")
	}
}

func TestFleetSupportsConcurrentReadsAndUpdates(t *testing.T) {
	now := time.Now()
	f := newFleet(3, newLockedRandom())
	var workers sync.WaitGroup
	errorsFound := make(chan error, 80)

	for worker := 0; worker < 8; worker++ {
		worker := worker
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 40; iteration++ {
				if worker%2 == 0 {
					_ = f.snapshots("")
					continue
				}
				_ = f.dueSnapshots(now.Add(time.Duration(iteration) * time.Minute))
			}
		}()
	}

	for iteration := 0; iteration < 40; iteration++ {
		iteration := iteration
		workers.Add(1)
		go func() {
			defer workers.Done()
			command := &smartcitypb.ConfigCommand{
				CommandId:        fmt.Sprintf("CMD-CONCURRENT-%03d", iteration),
				Timestamp:        now.Unix(),
				UpdateFrequency:  true,
				NewFrequencySecs: int32((iteration % 60) + 1),
				TargetDeviceId:   "parking_centro_01",
			}
			if _, err := f.applyCommand(command, now); err != nil {
				errorsFound <- err
			}
		}()
	}

	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("operação concorrente falhou: %v", err)
	}
}
