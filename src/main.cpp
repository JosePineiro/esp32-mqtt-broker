#include <Arduino.h>

void setup() {
  Serial.begin(115200);
  delay(1000);

  Serial.println("\n=== DETECCIÓN DE PSRAM ===");

  #ifdef ESP32
  if (psramFound()) {
    Serial.println("✅ PSRAM detectada!");
    Serial.print("Tamaño PSRAM: ");
    Serial.print(ESP.getPsramSize() / 1024);
    Serial.println(" KB");
  } else {
    Serial.println("❌ No se detecta PSRAM en este módulo.");
  }
  #else
  Serial.println("⚠️ No es un ESP32, no aplica.");
  #endif
}

void loop() {}

/*
#include <WiFi.h>
#include <TinyMqtt.h>
#include <esp_psram.h>
#include <vector>
#include <map>

// ====================================
// Configuración WiFi
// ====================================
const char* ssid = "TU_SSID";
const char* password = "TU_PASSWORD";

// ====================================
// Estructura para mensajes persistentes
// ====================================
struct PersistentMessage {
  String topic;
  String payload;
  uint16_t packetId;
  unsigned long timestamp;
  bool acked;
};

// ====================================
// Clase para gestionar persistencia en PSRAM
// ====================================
class MessageStore {
private:
  std::vector<PersistentMessage*> messages;
  uint16_t nextPacketId;
  
public:
  MessageStore() : nextPacketId(1) {
    if (!psramFound()) {
      Serial.println("ERROR: PSRAM no encontrada!");
      return;
    }
    Serial.printf("PSRAM disponible: %d bytes\n", ESP.getPsramSize());
  }
  
  uint16_t getNextPacketId() {
    uint16_t id = nextPacketId++;
    if (nextPacketId == 0) nextPacketId = 1;
    return id;
  }
  
  bool storeMessage(const String& topic, const String& payload, uint16_t packetId) {
    PersistentMessage* msg = (PersistentMessage*)ps_malloc(sizeof(PersistentMessage));
    if (!msg) {
      Serial.println("Error: No se pudo alocar memoria en PSRAM");
      return false;
    }
    
    new (msg) PersistentMessage();
    msg->topic = topic;
    msg->payload = payload;
    msg->packetId = packetId;
    msg->timestamp = millis();
    msg->acked = false;
    
    messages.push_back(msg);
    Serial.printf("Mensaje almacenado: ID=%d, Topic=%s, Size=%d bytes\n", 
                  packetId, topic.c_str(), payload.length());
    return true;
  }
  
  bool ackMessage(uint16_t packetId) {
    for (auto& msg : messages) {
      if (msg->packetId == packetId && !msg->acked) {
        msg->acked = true;
        Serial.printf("Mensaje ACK recibido: ID=%d\n", packetId);
        return true;
      }
    }
    return false;
  }
  
  std::vector<PersistentMessage*> getUnackedMessages() {
    std::vector<PersistentMessage*> unacked;
    for (auto& msg : messages) {
      if (!msg->acked) {
        unacked.push_back(msg);
      }
    }
    return unacked;
  }
  
  void cleanup(unsigned long maxAge = 60000) {
    auto it = messages.begin();
    while (it != messages.end()) {
      if ((*it)->acked && (millis() - (*it)->timestamp > maxAge)) {
        Serial.printf("Limpiando mensaje: ID=%d\n", (*it)->packetId);
        (*it)->~PersistentMessage();
        free(*it);
        it = messages.erase(it);
      } else {
        ++it;
      }
    }
  }
  
  size_t getStoredCount() { return messages.size(); }
  
  size_t getUnackedCount() {
    size_t count = 0;
    for (auto& msg : messages) {
      if (!msg->acked) count++;
    }
    return count;
  }
  
  ~MessageStore() {
    for (auto& msg : messages) {
      msg->~PersistentMessage();
      free(msg);
    }
  }
};

// ====================================
// Clase Broker con QoS 1
// ====================================
class QoS1Broker : public MqttBroker {
private:
  MessageStore* store;
  std::map<String, std::vector<MqttClient*>> subscriptions;
  
public:
  QoS1Broker(uint16_t port, MessageStore* msgStore) : MqttBroker(port), store(msgStore) {}
  
  void onPublish(MqttClient* client, const String& topic, const String& payload, uint8_t qos, bool retain) {
    Serial.printf("Publicación recibida - Topic: %s, QoS: %d, Payload: %s\n", 
                  topic.c_str(), qos, payload.c_str());
    
    if (qos == 1) {
      uint16_t packetId = store->getNextPacketId();
      store->storeMessage(topic, payload, packetId);
      sendPubAck(client, packetId);
    }
    
    distributeMessage(topic, payload, qos);
  }
  
  void onSubscribe(MqttClient* client, const String& topic) {
    Serial.printf("Nueva suscripción: %s\n", topic.c_str());
    subscriptions[topic].push_back(client);
  }
  
  void onUnsubscribe(MqttClient* client, const String& topic) {
    Serial.printf("Cancelar suscripción: %s\n", topic.c_str());
    auto& clients = subscriptions[topic];
    clients.erase(std::remove(clients.begin(), clients.end(), client), clients.end());
  }
  
private:
  void sendPubAck(MqttClient* client, uint16_t packetId) {
    Serial.printf("PUBACK enviado: ID=%d\n", packetId);
    store->ackMessage(packetId);
  }
  
  void distributeMessage(const String& topic, const String& payload, uint8_t qos) {
    for (auto& sub : subscriptions) {
      if (topicMatch(sub.first, topic)) {
        for (auto& client : sub.second) {
          if (client->connected()) {
            uint16_t packetId = (qos == 1) ? store->getNextPacketId() : 0;
            client->publish(topic, payload, qos, false);
            
            if (qos == 1) {
              Serial.printf("Mensaje QoS1 enviado a suscriptor: ID=%d\n", packetId);
            }
          }
        }
      }
    }
  }
  
  bool topicMatch(const String& filter, const String& topic) {
    if (filter == topic) return true;
    if (filter == "#") return true;
    
    int filterPos = 0, topicPos = 0;
    while (filterPos < filter.length() && topicPos < topic.length()) {
      if (filter[filterPos] == '+') {
        while (topicPos < topic.length() && topic[topicPos] != '/') topicPos++;
        filterPos++;
      } else if (filter[filterPos] == topic[topicPos]) {
        filterPos++;
        topicPos++;
      } else {
        return false;
      }
    }
    
    return filterPos == filter.length() && topicPos == topic.length();
  }
};

// ====================================
// Variables globales
// ====================================
MessageStore* messageStore;
QoS1Broker* broker;

// ====================================
// Setup
// ====================================
void setup() {
  Serial.begin(115200);
  delay(1000);
  
  Serial.println("\n=== Broker MQTT ESP32-S3 con QoS1 y PSRAM ===");
  
  if (!psramFound()) {
    Serial.println("ERROR CRÍTICO: PSRAM no disponible!");
    Serial.println("Verifica la configuración en platformio.ini");
    while(1) delay(1000);
  }
  
  messageStore = new MessageStore();
  
  Serial.printf("Conectando a WiFi: %s\n", ssid);
  WiFi.begin(ssid, password);
  
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  
  Serial.println("\nWiFi conectado!");
  Serial.print("IP del Broker: ");
  Serial.println(WiFi.localIP());
  
  broker = new QoS1Broker(1883, messageStore);
  broker->begin();
  
  Serial.println("Broker MQTT iniciado en puerto 1883");
  Serial.println("Características:");
  Serial.println("- QoS 0 y QoS 1 soportados");
  Serial.println("- Persistencia de mensajes en PSRAM");
  Serial.println("- Reenvío automático de mensajes no confirmados");
}

// ====================================
// Loop
// ====================================
void loop() {
  broker->loop();
  
  static unsigned long lastStats = 0;
  if (millis() - lastStats > 10000) {
    lastStats = millis();
    
    Serial.println("\n--- Estadísticas del Broker ---");
    Serial.printf("Clientes conectados: %d\n", broker->getClientCount());
    Serial.printf("Mensajes almacenados: %d\n", messageStore->getStoredCount());
    Serial.printf("Mensajes pendientes ACK: %d\n", messageStore->getUnackedCount());
    Serial.printf("PSRAM libre: %d bytes\n", ESP.getFreePsram());
    Serial.println("-------------------------------\n");
    
    messageStore->cleanup(60000);
  }
  
  static unsigned long lastRetry = 0;
  if (millis() - lastRetry > 5000) {
    lastRetry = millis();
    
    auto unacked = messageStore->getUnackedMessages();
    if (!unacked.empty()) {
      Serial.printf("Reenviando %d mensajes no confirmados...\n", unacked.size());
    }
  }
}
  */