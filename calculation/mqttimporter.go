package calculation

import (
	"context"
	"encoding/json"

	"at.ourproject/energystore/model"
	"github.com/golang/glog"
)

type MqttInverterMessage struct {
	data   *model.MqttEnergyResponse
	tenant string
}

//type MqttInverterImporter struct {
//	msgChan chan MqttInverterMessage
//	ctx     context.Context
//}
//
//func NewMqttInverterImporter(ctx context.Context) *MqttInverterImporter {
//	importer := &MqttInverterImporter{msgChan: make(chan MqttInverterMessage, 20), ctx: ctx}
//	go importer.process()
//	return importer
//}
//
//func (miv *MqttInverterImporter) Execute(msg mqtt.Message) {
//	tenant := mqttclient.TopicType(msg.Topic()).Tenant()
//	if len(tenant) == 0 {
//		return
//	}
//	data := decodeInverterMessage(msg.Payload())
//	if data == nil {
//		return
//	}
//
//	miv.msgChan <- MqttInverterMessage{data: data, tenant: tenant}
//}

var testInvCounter = 0

//func (miv *MqttInverterImporter) process() {
//	for {
//		select {
//		case msg := <-miv.msgChan:
//			glog.Infof("Execute Inverter Data Message for Topic (%v)\n", msg.tenant)
//			err := importEnergyV2(msg.tenant, "inverter", &msg.data.Message)
//			if err != nil {
//				glog.Error(err)
//			}
//			glog.Infof("Execution finished (Inv-Counter: %d)", testInvCounter)
//			testInvCounter += 1
//		case <-miv.ctx.Done():
//			break
//		}
//	}
//}

type MqttMessage struct {
	data   *model.MqttEnergyMessage
	tenant string
	ecId   string
}

type MqttEnergyImporter struct {
	msgChan chan MqttMessage
	ctx     context.Context
}

//func NewMqttEnergyImporter(ctx context.Context) *MqttEnergyImporter {
//	importer := &MqttEnergyImporter{msgChan: make(chan MqttMessage, 20), ctx: ctx}
//	go importer.process()
//	return importer
//}
//
//var gloablReceivedMsg int = 0
//
//func (mw *MqttEnergyImporter) Execute(msg mqtt.Message) {
//	gloablReceivedMsg = gloablReceivedMsg + 1
//	tenant := mqttclient.TopicType(msg.Topic()).Tenant()
//	if len(tenant) == 0 {
//		return
//	}
//	data := decodeMessage(msg.Payload())
//	if data == nil {
//		return
//	}
//
//	mw.msgChan <- MqttMessage{data: data, tenant: tenant, ecId: data.EcId}
//	glog.V(4).Infof("Received Messages %d\n", gloablReceivedMsg)
//	//msg.Ack()
//}
//
//var testCounter int64 = 0
//
//func (mw *MqttEnergyImporter) process() {
//	for {
//		select {
//		case msg := <-mw.msgChan:
//			glog.Infof("Execute Energy Data Message for Topic (%v)", msg.tenant)
//			err := importEnergyV2(msg.tenant, msg.ecId, msg.data)
//			if err != nil {
//				glog.Error(err)
//			}
//			glog.Infof("Execution finished (%d - %v)", testCounter, msg.tenant)
//			testCounter += 1
//		case <-mw.ctx.Done():
//			break
//		}
//	}
//}

func decodeInverterMessage(msg []byte) *model.MqttEnergyResponse {
	//m := model.MqttEnergyResponse{}
	m := model.MqttEnergyResponse{}
	err := json.Unmarshal(msg, &m)
	if err != nil {
		glog.Errorf("Error decoding MQTT message. %s", err.Error())
		return nil
	}
	return &m
}
