#pragma once

#define TINY_MQTT_SERVER_REVISION "1.0.0-esp32s3"
#define TINY_MQTT_SERVER

#include <WiFi.h>
#include <AsyncTCP.h>
#include <vector>
#include <set>
#include <string>
#include <map>
#include <memory>
#include <esp_log.h>
#include "StringIndexer.h"

/**
 * @brief MQTT error codes used by this minimal broker implementation.
 */
enum MqttError
{
  MqttOk = 0,
  MqttNowhereToSend = 1,
  MqttInvalidMessage = 2,
};

/**
 * @brief Topic wrapper that extends IndexedString to provide MQTT topic utilities.
 */
class Topic : public IndexedString
{
public:
  Topic(const std::string &str) : IndexedString(str) {}
  Topic(const char *str, uint8_t len) : IndexedString(str, len) {}
  Topic(const char *str) : Topic(str, static_cast<uint8_t>(strlen(str))) {}

  const char *c_str() const { return str().c_str(); }

  size_t length() const { return static_cast<uint8_t>(str().length()); }

  /**
   * @brief Check whether this topic (which may contain wildcards) matches another topic.
   * @param other Topic to test against.
   * @return true if match, false otherwise.
   */
  bool matches(const Topic &other) const;
};

class MqttClient;

/**
 * @brief Represents an MQTT message with simple buffering and encoding utilities.
 */
class MqttMessage
{
  static const uint16_t MAX_BUFFER_LENGTH = 4096;

public:
  enum Type
  {
    Unknown = 0,
    Connect = 0x10,
    ConnAck = 0x20,
    Publish = 0x30,
    PubAck = 0x40,
    PubRec = 0x50,
    PubRel = 0x60,
    PubComp = 0x70,
    Subscribe = 0x80,
    SubAck = 0x90,
    UnSubscribe = 0xA0,
    UnSuback = 0xB0,
    PingReq = 0xC0,
    PingResp = 0xD0,
    Disconnect = 0xE0
  };

  enum State
  {
    FixedHeader = 0,
    Length = 1,
    VariableHeader = 2,
    PayLoad = 3,
    Complete = 4,
    Error = 5,
    Create = 6
  };

  MqttMessage() { reset(); }
  MqttMessage(Type type, uint8_t bitsD3D0 = 0);
  MqttMessage(const MqttMessage &mqttMessage);

  /** Feed a received byte to this message parser. */
  void incoming(char byte);

  /** Add single byte to the message (alias to incoming). */
  void add(char byte) { incoming(byte); }

  /** Add a binary buffer; optionally add two-byte length header. */
  void add(const char *p, size_t len, bool addLength = true);

  /** Add std::string as UTF-8 payload (with length). */
  // void add(const std::string &str) { add(str.c_str(), str.length()); }

  /** Add topic (IndexedString wrapper) to the buffer. */
  void add(const Topic &topic) { add(topic.c_str(), topic.length()); }

  /** Pointer to the end of internal buffer. */
  const char *end() const { return &buffer_[0] + buffer_.size(); }

  /** Pointer to the start of the variable header within the buffer. */
  const char *getVariableHeader() const { return &buffer_[vheader_]; }

  /** Finalize the message length fields and mark complete. */
  void complete();

  /** Mark Publish message as retained (sets retained flag in first byte). */
  void setRetained();

  /**
   * @brief Set QoS bits in the fixed header (1 or 2).
   * This only modifies the header bits; it doesn't append Packet ID bytes.
   */
  void setQos(uint8_t level);

  /** Append only the 2-byte Packet Identifier to the buffer. */
  void addPacketId(uint16_t packetId);

  /** Reset the message state and clear buffer. */
  void reset();

  /** Read a two-byte length-prefixed string: advances buffer pointer by 2 and returns length. */
  static inline uint32_t getSize(const char *buffer)
  {
    const unsigned char *bun = (const unsigned char *)buffer;
    return (*bun << 8) | bun[1];
  }

  /** Extract a length-prefixed string from a pointer into the variable header / payload.
   *  If successful, `buff` is advanced past the two-byte length and `len` is set.
   */
  bool getString(const char *&buff, uint16_t &len);

  /** Returns the inferred packet Type if complete, otherwise Unknown. */
  Type type() const
  {
    return state_ == Complete ? static_cast<Type>(buffer_[0] & 0xF0) : Unknown;
  }

  /** Lower 4 flag bits from the fixed header. */
  uint8_t flags() const { return static_cast<uint8_t>(buffer_[0] & 0x0F); }

  /** Create a new message with an initial fixed header byte and placeholder length. */
  void create(Type type);

  /** Send this encoded message to a client. */
  MqttError sendTo(MqttClient *);

  /** Simple hex dump helper for debugging. */
  void hexdump(const char *prefix = nullptr) const;

  MqttMessage &operator=(MqttMessage &&m) noexcept;

private:
  void encodeLength();
  std::string buffer_;
  uint8_t vheader_ = 0;
  uint16_t size_ = 0;
  State state_ = FixedHeader;
};

class MqttBroker;

/**
 * @brief Represents a connected MQTT client (server side).
 *
 * This class handles parsing client messages and sending data back to the client.
 */
class MqttClient
{
  enum AckReturnCode
  {
    ConnectionAccepted = 0,
    UnacceptableProtocolVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUserOrPassword = 4,
    NotAuthorized = 5
  };

  enum Flags
  {
    FlagUserName = 128,
    FlagPassword = 64,
    FlagWillRetain = 32,
    FlagWillQos = 16 | 8,
    FlagWill = 4,
    FlagCleanSession = 2,
    FlagReserved = 1
  };

  enum ClientFlags
  {
    ClientFlagNone = 0,
    ClientFlagConnected = 1,
    ClientFlagToDelete = 2,
    ClientFlagCleanSession = 4
  };

public:
  ~MqttClient();

  /** Check whether the TCP client is currently connected. */
  bool connected() const { return tcpClient_ && tcpClient_->connected(); }

  /** Write raw bytes to the client's TCP socket. */
  void write(const char *buf, size_t length)
  {
    if (tcpClient_)
      tcpClient_->write(buf, length);
  }

  /** Return client identifier std::string. */
  const std::string &id() const { return clientId_; }

  /** Close the client connection. Optionally send a DISCONNECT control packet first. */
  void Close(bool sendDisconnect = true);

  /** Return keep-alive interval in seconds. */
  uint32_t keepAlive() const { return keepAlive_; }

  /** Query whether the session is a clean session. */
  bool isCleanSession() const { return clientFlags_ & ClientFlagCleanSession; }

private:
  friend class MqttBroker;

  explicit MqttClient(MqttBroker *broker, AsyncClient *client);

  bool mqttConnected() const { return clientFlags_ & ClientFlagConnected; }
  void setClientFlag(ClientFlags f) { clientFlags_ |= f; }
  void resetClientFlag(ClientFlags f) { clientFlags_ &= ~f; }

  void refreshAlive(uint32_t moreSeconds);
  void processMessage(MqttMessage *message);
  MqttError publishIfSubscribed(const Topic &topic, MqttMessage &msg);
  bool isSubscribedTo(const Topic &topic) const;
  void sendPendingMessages();
  void storePendingMessage(const Topic &topic, const MqttMessage &msg, uint8_t qos);

  static void onDataCallback(void *client_ptr, AsyncClient *, void *data, size_t len);
  static void onDisconnectCallback(void *client_ptr, AsyncClient *);

  uint8_t clientFlags_ = ClientFlagNone;
  char mqttFlags_ = 0;
  uint32_t keepAlive_ = 30;
  uint32_t alive_ = 0;
  uint16_t nextPacketId_ = 1;
  MqttMessage mqttMessage_;

  MqttBroker *mqttBroker_ = nullptr;
  AsyncClient *tcpClient_ = nullptr;
  std::set<Topic> subscriptions_;
  std::map<uint16_t, uint16_t> pendingPackets_;
  std::string clientId_;
};

/** Message retained on topics. */
struct RetainedMessage
{
  std::string payload;
  uint8_t qos;

  RetainedMessage() : qos(0) {}
  RetainedMessage(const std::string &p, uint8_t q)
      : payload(p), qos(q) {}
};

/** Message stored for offline clients with persistent sessions. */
struct PersistentMessage
{
  std::string topic;
  std::string payload;
  uint8_t qos;
  uint16_t packetId;

  PersistentMessage() : qos(0), packetId(0) {}
  PersistentMessage(const std::string &t, const std::string &p, uint8_t q, uint16_t pid)
      : topic(t), payload(p), qos(q), packetId(pid) {}
};

/**
 * @brief Minimal in-memory MQTT broker suitable for constrained devices.
 *
 * The broker keeps track of connected clients, retained messages, and
 * persistent session storage (stored pending messages & subscriptions).
 */
class MqttBroker
{
public:
  explicit MqttBroker(uint16_t port, uint8_t retainSize = 100);
  ~MqttBroker();

  /** Start listening for incoming TCP client connections. */
  void Start();

  /** Periodic work; cleans up disconnected clients. */
  void Poll();

  /** Current number of connected clients. */
  size_t GetClientCount() const { return mqttClients_.size(); }

  /** Return a copy of the client pointers vector. */
  const std::vector<MqttClient *> GetClients() const { return mqttClients_; }

  /** Set username required for authentication (optional). */
  void SetAuthUser(const char *user);

  /** Set password required for authentication (optional). */
  void SetAuthPassword(const char *password);

  /** Save session (subscriptions & pending messages) for a client. */
  void SaveSession(MqttClient *client);

  /** Restore session when a client reconnects. */
  void RestoreSession(MqttClient *client);

  /** Clear persistent session data for a given client ID. */
  void ClearSession(const std::string &clientId);

  /** Set a retained message for a topic (empty payload clears retained). */
  void SetRetained(const Topic &topic, const std::string &payload, uint8_t qos = 0);

  /** Remove retained message for a topic. */
  void ClearRetained(const Topic &topic);

  /** Send retained messages that match a subscription to a client. */
  void SendRetainedMessages(MqttClient *client, const Topic &subscription);

  /** Store a message for offline clients whose session is persistent and subscribed. */
  void StoreMessageForOfflineClients(const Topic &topic, MqttMessage &msg, uint8_t qos);

  /** Allocate next packet identifier (wraps 0 to 1). */
  uint16_t GetNextPacketId();

private:
  friend class MqttClient;

  const char *authUser_ = "";
  const char *authPassword_ = "";

  /** Callback invoked by AsyncServer when a new TCP client connects. */
  static void onClientCallback(void *broker_ptr, AsyncClient *tcpClient);

  bool checkUser(const char *user, uint8_t len) const { return compareString(authUser_, user, len); }
  bool checkPassword(const char *password, uint8_t len) const { return compareString(authPassword_, password, len); }

  MqttError Publish(const MqttClient *source, const Topic &topic, MqttMessage &mqttMessage);
  MqttError Subscribe(MqttClient *, const Topic &topic, uint8_t qos);
  void AddClient(MqttClient *client);
  void RemoveClient(MqttClient *client);
  bool compareString(const char *good, const char *str, uint8_t strLen) const;

  std::vector<MqttClient *> mqttClients_;
  std::unique_ptr<AsyncServer> tcpServer_;

  std::map<std::string, RetainedMessage> retainedMessages_;
  uint8_t retainSize_;

  uint16_t nextPacketId_ = 1;

  // Persistent sessions: clientId -> stored pending messages
  std::map<std::string, std::vector<PersistentMessage>> persistentSessions_;

  // Stored subscriptions for persistent sessions: clientId -> topics
  std::map<std::string, std::set<Topic>> sessionSubscriptions_;
};
