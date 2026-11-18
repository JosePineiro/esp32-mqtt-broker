#include "TinyMqttServer.h"
#include <cstring>
#include <utility>

/* ---------------------------
   MqttMessage implementation
   --------------------------- */

MqttMessage::MqttMessage(Type type, uint8_t bitsD3D0)
{
  create(type);
  buffer_[0] |= (bitsD3D0 & 0xF);
}

MqttMessage::MqttMessage(const MqttMessage &m)
    : buffer_(m.buffer_), vheader_(m.vheader_), size_(m.size_), state_(m.state_)
{
}

void MqttMessage::incoming(char byte)
{
  buffer_ += byte;
  switch (state_)
  {
  case FixedHeader:
    size_ = MAX_BUFFER_LENGTH;
    state_ = Length;
    break;

  case Length:
    if (size_ == MAX_BUFFER_LENGTH)
      size_ = byte & 0x7F;
    else
      size_ += static_cast<uint16_t>(byte & 0x7F) << 7;

    if (size_ > MAX_BUFFER_LENGTH)
      state_ = Error;
    else if ((byte & 0x80) == 0)
    {
      vheader_ = buffer_.length();
      state_ = size_ == 0 ? Complete : VariableHeader;
      if (size_ > 0)
        buffer_.reserve(size_);
    }
    break;

  case VariableHeader:
  case PayLoad:
    if (--size_ == 0)
      state_ = Complete;
    break;

  case Create:
    size_++;
    break;

  case Complete:
  default:
    reset();
    break;
  }

  if (buffer_.length() > MAX_BUFFER_LENGTH)
    reset();
}

void MqttMessage::reset()
{
  buffer_.clear();
  state_ = FixedHeader;
  size_ = 0;
}

void MqttMessage::add(const char *p, size_t len, bool addLength)
{
  if (addLength)
  {
    buffer_.reserve(buffer_.length() + 2);
    incoming(len >> 8);
    incoming(len & 0xFF);
  }
  while (len--)
    incoming(*p++);
}

void MqttMessage::encodeLength()
{
  if (state_ != Complete)
  {
    int length = static_cast<int>(buffer_.size()) - 3;
    if (length <= 0x7F)
    {
      buffer_.erase(1, 1);
      buffer_[1] = static_cast<char>(length);
      vheader_ = 2;
    }
    else
    {
      buffer_[1] = static_cast<char>(0x80 | (length & 0x7F));
      buffer_[2] = static_cast<char>((length >> 7) & 0xFF);
      vheader_ = 3;
    }
    state_ = Complete;
  }
}

void MqttMessage::complete()
{
  encodeLength();
}

void MqttMessage::setRetained()
{
  if ((buffer_[0] & 0xF0) == Publish)
    buffer_[0] |= 1;
}

void MqttMessage::setQos(uint8_t level)
{
  if (level > 0 && level <= 2)
  {
    buffer_[0] &= 0b11111001; // Clear QoS bits
    buffer_[0] |= (level << 1);
  }
}

void MqttMessage::addPacketId(uint16_t packetId)
{
  incoming(static_cast<char>(packetId >> 8));
  incoming(static_cast<char>(packetId & 0xFF));
}

void MqttMessage::create(Type type)
{
  buffer_ = (decltype(buffer_)::value_type)type;
  buffer_ += '\0';
  buffer_ += '\0';
  vheader_ = 3;
  size_ = 0;
  state_ = Create;
}

MqttMessage &MqttMessage::operator=(MqttMessage &&m) noexcept
{
  buffer_ = std::move(m.buffer_);
  vheader_ = m.vheader_;
  size_ = m.size_;
  state_ = m.state_;
  return *this;
}

void MqttMessage::hexdump(const char *prefix) const
{
  if (!prefix)
  {
    log_d("hexdump:");
  }
  else
  {
    log_d("%s:", prefix);
  }
  // minimal hexdump (prints length)
  log_d("  bytes: %d", (int)buffer_.size());
}

MqttError MqttMessage::sendTo(MqttClient *client)
{
  if (buffer_.size() > 0)
  {
    encodeLength();
    client->write(&buffer_[0], buffer_.size());
    return MqttOk;
  }
  return MqttInvalidMessage;
}

bool MqttMessage::getString(const char *&buff, uint16_t &len)
{
  if (end() - buff < 2)
  {
    return false;
  }
  len = getSize(buff);
  if (end() - buff < static_cast<int>(len) + 2)
  {
    return false;
  }
  buff += 2;
  return true;
}

/* ---------------------------
   MqttBroker / MqttClient
   --------------------------- */

MqttBroker::MqttBroker(uint16_t port, uint8_t retainSize)
    : retainSize_(retainSize), tcpServer_(std::make_unique<AsyncServer>(port))
{
  log_d("New broker on port %d", port);
}

MqttBroker::~MqttBroker()
{
  for (auto client : mqttClients_)
  {
    client->mqttBroker_ = nullptr;
    if (client->clientFlags_ & MqttClient::ClientFlagToDelete)
      delete client;
  }
  mqttClients_.clear();
  // unique_ptr will delete server automatically
}

MqttClient::MqttClient(MqttBroker *broker, AsyncClient *newClient)
    : mqttBroker_(broker), tcpClient_(newClient)
{
  log_d("MqttClient created");
  if (tcpClient_)
  {
    tcpClient_->onData(onDataCallback, this);
    tcpClient_->onDisconnect(onDisconnectCallback, this);
  }
  alive_ = millis() + 5000;
}

MqttClient::~MqttClient()
{
  Close();
  log_d("MqttClient deleted");
}

void MqttClient::Close(bool sendDisconnect)
{
  log_d("Close %s", id().c_str());
  resetClientFlag(ClientFlagConnected);

  if (tcpClient_)
  {
    if (sendDisconnect && tcpClient_->connected())
    {
      MqttMessage message;
      message.create(MqttMessage::Type::Disconnect);
      message.sendTo(this);
    }
    tcpClient_->close();
  }

  if (mqttBroker_)
  {
    if (!isCleanSession())
    {
      mqttBroker_->SaveSession(this);
    }
    else
    {
      mqttBroker_->ClearSession(clientId_);
    }
    mqttBroker_->RemoveClient(this);
    mqttBroker_ = nullptr;
  }
}

void MqttBroker::Start()
{
  log_d("MqttBroker::Start");
  tcpServer_->onClient(onClientCallback, this);
  tcpServer_->begin();
}

void MqttBroker::Poll()
{
  for (size_t i = 0; i < mqttClients_.size(); i++)
  {
    if (!mqttClients_[i]->connected())
    {
      log_d("Removing disconnected client: %s", mqttClients_[i]->id().c_str());
      delete mqttClients_[i];
      break;
    }
  }
}

void MqttBroker::AddClient(MqttClient *client)
{
  log_d("AddClient");
  mqttClients_.push_back(client);
}

void MqttBroker::RemoveClient(MqttClient *remove)
{
  for (auto it = mqttClients_.begin(); it != mqttClients_.end(); it++)
  {
    if (*it == remove)
    {
      mqttClients_.erase(it);
      log_d("Client removed");
      return;
    }
  }
}

void MqttBroker::onClientCallback(void *broker_ptr, AsyncClient *client)
{
  log_d("New client connection");
  auto broker = static_cast<MqttBroker *>(broker_ptr);
  auto mqttClient = new MqttClient(broker, client);
  mqttClient->setClientFlag(MqttClient::ClientFlagToDelete);
  broker->AddClient(mqttClient);
}

void MqttClient::onDataCallback(void *client_ptr, AsyncClient *ac, void *data, size_t len)
{
  MqttClient *client = static_cast<MqttClient *>(client_ptr);
  char *char_ptr = static_cast<char *>(data);

  while (len > 0)
  {
    client->mqttMessage_.incoming(*char_ptr++);
    if (client->mqttMessage_.type())
    {
      client->processMessage(&client->mqttMessage_);
      client->mqttMessage_.reset();
    }
    len--;
  }
}

void MqttClient::onDisconnectCallback(void *client_ptr, AsyncClient *ac)
{
  MqttClient *client = static_cast<MqttClient *>(client_ptr);
  client->resetClientFlag(MqttClient::ClientFlagConnected);
  log_d("Client disconnected");
}

void MqttBroker::SetRetained(const Topic &topic, const std::string &payload, uint8_t qos)
{
  log_d("Storing retained message for topic: %s", topic.c_str());

  if (payload.empty())
  {
    ClearRetained(topic);
  }
  else
  {
    retainedMessages_[std::string(topic.c_str())] = RetainedMessage(payload, qos);
  }
}

void MqttBroker::ClearRetained(const Topic &topic)
{
  log_d("Clearing retained message for topic: %s", topic.c_str());
  auto it = retainedMessages_.find(std::string(topic.c_str()));
  if (it != retainedMessages_.end())
  {
    retainedMessages_.erase(it);
  }
}

void MqttBroker::SendRetainedMessages(MqttClient *client, const Topic &subscription)
{
  log_d("Sending retained messages matching: %s", subscription.c_str());

  for (auto &pair : retainedMessages_)
  {
    Topic retainedTopic(pair.first.c_str());
    if (subscription.matches(retainedTopic))
    {
      log_d("  Sending retained: %s (QoS: %d, Payload: %s)",
            retainedTopic.c_str(), pair.second.qos, pair.second.payload.c_str());

      MqttMessage msg(MqttMessage::Publish);

      // Add topic (length-prefixed)
      msg.add(retainedTopic);

      // Packet ID if QoS > 0
      if (pair.second.qos > 0)
      {
        msg.addPacketId(GetNextPacketId());
      }

      // Add raw payload (no length prefix)
      msg.add(pair.second.payload.c_str(), pair.second.payload.length(), false);

      msg.setQos(pair.second.qos);
      msg.setRetained();

      msg.sendTo(client);
    }
  }
}

void MqttBroker::SaveSession(MqttClient *client)
{
  log_d("Saving session for: %s", client->id().c_str());

  sessionSubscriptions_[client->id()] = client->subscriptions_;

  if (!client->pendingPackets_.empty())
  {
    log_d("  Pending packets to save: %d", client->pendingPackets_.size());
  }
}

void MqttBroker::RestoreSession(MqttClient *client)
{
  log_d("Restoring session for: %s", client->id().c_str());

  auto sub_it = sessionSubscriptions_.find(client->id());
  if (sub_it != sessionSubscriptions_.end())
  {
    client->subscriptions_ = sub_it->second;
    log_d("  Restored %d subscriptions", client->subscriptions_.size());

    for (const auto &topic : client->subscriptions_)
    {
      SendRetainedMessages(client, topic);
    }
  }

  auto msg_it = persistentSessions_.find(client->id());
  if (msg_it != persistentSessions_.end() && !msg_it->second.empty())
  {
    log_d("  Restoring %d pending messages", msg_it->second.size());

    for (const auto &pm : msg_it->second)
    {
      MqttMessage msg(MqttMessage::Publish);
      msg.add(Topic(pm.topic.c_str()));

      if (pm.qos > 0)
      {
        msg.addPacketId(pm.packetId);
      }

      msg.add(pm.payload.c_str(), pm.payload.length(), false);
      msg.setQos(pm.qos);
      msg.sendTo(client);
    }

    persistentSessions_.erase(msg_it);
  }
}

void MqttBroker::ClearSession(const std::string &clientId)
{
  log_d("Clearing session for: %s", clientId.c_str());
  sessionSubscriptions_.erase(clientId);
  persistentSessions_.erase(clientId);
}

MqttError MqttBroker::Subscribe(MqttClient *client, const Topic &topic, uint8_t qos)
{
  log_d("subscribe to %s", topic.str().c_str());
  SendRetainedMessages(client, topic);
  return MqttOk;
}

MqttError MqttBroker::Publish(const MqttClient *source, const Topic &topic, MqttMessage &msg)
{
  log_d("publish to %s", topic.str().c_str());

  // Handle retained flag
  if (msg.flags() & 1)
  {
    const char *header = msg.getVariableHeader();
    uint16_t topic_len = 0;

    const char *payload_ptr = header;
    if (msg.getString(payload_ptr, topic_len))
    {
      Topic pubTopic(payload_ptr, topic_len);
      payload_ptr += topic_len;

      uint8_t qosBits = (msg.flags() >> 1) & 0x03;
      if (qosBits > 0)
      {
        payload_ptr += 2;
      }

      size_t payload_len = msg.end() - payload_ptr;
      SetRetained(pubTopic, std::string(payload_ptr, payload_len), qosBits);
    }
  }

  for (auto client : mqttClients_)
  {
    client->publishIfSubscribed(topic, const_cast<MqttMessage &>(msg));
  }

  uint8_t qos = (msg.flags() >> 1) & 0x03;
  if (qos > 0)
  {
    log_d("  Storing message for potential offline clients (QoS %d)", qos);
    StoreMessageForOfflineClients(topic, msg, qos);
  }

  return MqttOk;
}

void MqttBroker::StoreMessageForOfflineClients(const Topic &topic, MqttMessage &msg, uint8_t qos)
{
  log_d("  Checking persistent sessions for topic: %s", topic.c_str());

  for (auto &session_pair : sessionSubscriptions_)
  {
    const std::string &clientId = session_pair.first;
    const auto &subscriptions = session_pair.second;

    for (const auto &subscription : subscriptions)
    {
      if (subscription.matches(topic))
      {
        log_d("    Offline client '%s' subscribed to: %s", clientId.c_str(), topic.c_str());

        const char *header = msg.getVariableHeader();
        uint16_t topic_len = 0;
        const char *payload_ptr = header;

        if (msg.getString(payload_ptr, topic_len))
        {
          payload_ptr += topic_len;

          uint16_t packetId = 0;
          if (qos > 0)
          {
            packetId = (static_cast<uint16_t>(static_cast<unsigned char>(payload_ptr[0])) << 8) |
                       static_cast<unsigned char>(payload_ptr[1]);
            payload_ptr += 2;
          }

          size_t payload_len = msg.end() - payload_ptr;
          std::string payload(payload_ptr, payload_len);

          PersistentMessage pm(std::string(topic.c_str()), payload, qos, packetId);
          persistentSessions_[clientId].push_back(pm);
          log_d("      Stored message (packets: %d)", (int)persistentSessions_[clientId].size());
        }
        break;
      }
    }
  }
}

bool MqttBroker::compareString(const char *good, const char *str, uint8_t len) const
{
  while (len-- && *good++ == *str++)
    ;
  return *good == 0;
}

void MqttClient::refreshAlive(uint32_t more_seconds)
{
  if (keepAlive_)
    alive_ = millis() + 1000 * (keepAlive_ + more_seconds);
  else
    alive_ = 0;
}

void MqttClient::storePendingMessage(const Topic &topic, const MqttMessage &msg, uint8_t qos)
{
  if (qos > 0)
  {
    pendingPackets_[nextPacketId_] = nextPacketId_;
    nextPacketId_++;
    if (nextPacketId_ == 0)
      nextPacketId_ = 1;
    log_d("Stored pending message with packet ID: %d", nextPacketId_ - 1);
  }
}

void MqttClient::sendPendingMessages()
{
  log_d("Sending pending messages for: %s", clientId_.c_str());
  log_d("  Total pending packets: %d", pendingPackets_.size());
}

void MqttClient::processMessage(MqttMessage *mesg)
{
  auto header = mesg->getVariableHeader();
  const char *payload;
  uint16_t len;
  bool closeAfter = true;
  MqttClient::AckReturnCode ackReturnCode = MqttClient::ConnectionAccepted;

  switch (mesg->type())
  {
  case MqttMessage::Type::Connect:
    if (mqttConnected())
      break;

    mqttFlags_ = header[7];
    keepAlive_ = MqttMessage::getSize(header + 8);

    if (strncmp("MQTT", header + 2, 4) != 0 || header[6] != 0x04)
    {
      ackReturnCode = MqttClient::UnacceptableProtocolVersion;
      break;
    }

    payload = header + 10;

    if (!mesg->getString(payload, len))
    {
      ackReturnCode = MqttClient::IdentifierRejected;
      break;
    }
    clientId_ = std::string(payload, len);
    payload += len;

    for (auto existingClient : mqttBroker_->mqttClients_)
    {
      if (existingClient != this && existingClient->connected() && existingClient->id() == clientId_)
      {
        log_d("Client ID already connected, rejecting connection");
        ackReturnCode = MqttClient::IdentifierRejected;
        break;
      }
    }

    if (ackReturnCode == MqttClient::IdentifierRejected)
      break;

    if (!(mqttFlags_ & MqttClient::FlagCleanSession))
    {
      resetClientFlag(MqttClient::ClientFlagCleanSession);
      log_d("Clean session: FALSE (persistent session enabled)");
    }
    else
    {
      setClientFlag(MqttClient::ClientFlagCleanSession);
      log_d("Clean session: TRUE");
    }

    if (mqttFlags_ & MqttClient::FlagWill)
    {
      if (!mesg->getString(payload, len))
      {
        ackReturnCode = MqttClient::NotAuthorized;
        break;
      }
      payload += len;

      if (!mesg->getString(payload, len))
      {
        ackReturnCode = MqttClient::NotAuthorized;
        break;
      }
      payload += len;
    }

    if (mqttBroker_->authUser_[0] != '\0')
    {
      const bool hasUserFlag = (mqttFlags_ & MqttClient::FlagUserName) != 0;
      const bool gotUser = hasUserFlag && mesg->getString(payload, len);
      const bool userOk = gotUser && mqttBroker_->checkUser(payload, len);

      if (!userOk)
      {
        if (!hasUserFlag)
          log_i("[MQTT] AUTH FAIL (user) - No username provided (expected '%s')", mqttBroker_->authUser_);
        else if (!gotUser)
          log_i("[MQTT] AUTH FAIL (user) - Malformed username field");
        else
          log_i("[MQTT] AUTH FAIL (user) - Provided user: '%.*s' (expected '%s')", static_cast<int>(len), payload, mqttBroker_->authUser_);

        ackReturnCode = MqttClient::BadUserOrPassword;
        break;
      }
      payload += len;
    }

    if (mqttBroker_->authPassword_[0] != '\0')
    {
      const bool hasPassFlag = (mqttFlags_ & MqttClient::FlagPassword) != 0;
      const bool gotPass = hasPassFlag && mesg->getString(payload, len);
      const bool passOk = gotPass && mqttBroker_->checkPassword(payload, len);

      if (!passOk)
      {
        if (!hasPassFlag)
          log_i("[MQTT] AUTH FAIL (password) - No password provided");
        else if (!gotPass)
          log_i("[MQTT] AUTH FAIL (password) - Malformed password field");
        else
          log_i("[MQTT] AUTH FAIL (password) - Wrong password: '%.*s'", static_cast<int>(len), payload);

        ackReturnCode = MqttClient::BadUserOrPassword;
        break;
      }
    }

    closeAfter = false;
    setClientFlag(MqttClient::ClientFlagConnected);

    {
      MqttMessage msg(MqttMessage::Type::ConnAck);
      msg.add(0);
      msg.add(0);
      msg.sendTo(this);
    }

    if (!isCleanSession())
    {
      mqttBroker_->RestoreSession(this);
    }
    else
    {
      mqttBroker_->ClearSession(clientId_);
    }

    refreshAlive(0);
    log_d("Client %s connected", clientId_.c_str());
    break;

  case MqttMessage::Type::PingResp:
  {
    closeAfter = false;
    refreshAlive(0);
    break;
  }

  case MqttMessage::Type::PingReq:
  {
    if (mqttConnected() && tcpClient_)
    {
      MqttMessage pingresp(MqttMessage::Type::PingResp);
      pingresp.sendTo(this);
      closeAfter = false;
      refreshAlive(0);
    }
    break;
  }

  case MqttMessage::Type::Subscribe:
  case MqttMessage::Type::UnSubscribe:
  {
    if (!mqttConnected())
      break;

    payload = header + 2;
    std::string qoss;

    while (payload < mesg->end())
    {
      mesg->getString(payload, len);
      Topic topic(payload, len);
      payload += len;

      if (mesg->type() == MqttMessage::Type::Subscribe)
      {
        uint8_t qos = *payload++;
        qoss.push_back(qos != 0 ? 0x80 : qos);
        subscriptions_.insert(topic);
        mqttBroker_->Subscribe(this, topic, qos);
      }
      else
      {
        subscriptions_.erase(topic);
      }
    }

    closeAfter = false;
    MqttMessage ack(mesg->type() == MqttMessage::Type::Subscribe ? MqttMessage::Type::SubAck : MqttMessage::Type::UnSuback);
    ack.add(header[0]);
    ack.add(header[1]);
    ack.add(qoss.c_str(), qoss.size(), false);
    ack.sendTo(this);
    refreshAlive(0);
    break;
  }

  case MqttMessage::Type::Publish:
  {
    if (!mqttConnected())
      break;

    uint8_t qos = (mesg->flags() >> 1) & 0x03;
    payload = header;
    uint16_t topic_len = 0;

    if (!mesg->getString(payload, topic_len))
      break;

    Topic publishedTopic(payload, topic_len);
    payload += topic_len;

    const char *ID = nullptr;
    if (qos)
    {
      ID = payload;
      payload += 2;
    }

    if (qos == 1 && ID)
    {
      MqttMessage msg(MqttMessage::Type::PubAck);
      msg.add(ID[0]);
      msg.add(ID[1]);
      msg.sendTo(this);
    }

    if (mqttBroker_)
      mqttBroker_->Publish(this, publishedTopic, *mesg);

    closeAfter = false;
    refreshAlive(0);
    break;
  }

  case MqttMessage::Type::PubAck:
  {
    if (mqttConnected())
    {
      uint16_t packetId = (static_cast<unsigned char>(header[0]) << 8) | static_cast<unsigned char>(header[1]);
      auto it = pendingPackets_.find(packetId);
      if (it != pendingPackets_.end())
      {
        log_d("PubAck received for packet %d", packetId);
        pendingPackets_.erase(it);
      }
      closeAfter = false;
      refreshAlive(0);
    }
    break;
  }

  case MqttMessage::Type::Disconnect:
  {
    if (mqttConnected())
    {
      resetClientFlag(MqttClient::ClientFlagConnected);
      Close(false);
      closeAfter = false;
    }
    break;
  }

  default:
    break;
  }

  if (ackReturnCode != 0)
  {
    MqttMessage msg(MqttMessage::Type::ConnAck);
    msg.add(0);
    msg.add(ackReturnCode);
    msg.sendTo(this);
    Close(false);
  }

  if (closeAfter)
    Close();
}

/* Topic matching with + and # wildcards */
bool Topic::matches(const Topic &topic) const
{
  if (getIndex() == topic.getIndex())
    return true;
  const char *p1 = c_str();
  const char *p2 = topic.c_str();

  if (p1 == p2)
    return true;
  if (*p2 == '$' && *p1 != '$')
    return false;

  while (*p1 && *p2)
  {
    if (*p1 == '+')
    {
      ++p1;
      if (*p1 && *p1 != '/')
        return false;
      if (*p1)
        ++p1;
      while (*p2 && *p2++ != '/')
        ;
    }
    else if (*p1 == '#')
    {
      return *++p1 == 0;
    }
    else if (*p1 == *p2)
    {
      ++p1;
      ++p2;
    }
    else
      return false;
  }
  if (*p1 == '/' && p1[1] == '#' && p1[2] == 0)
    return true;
  return *p1 == 0 && *p2 == 0;
}

MqttError MqttClient::publishIfSubscribed(const Topic &topic, MqttMessage &msg)
{
  if (isSubscribedTo(topic))
    return tcpClient_ ? msg.sendTo(this) : MqttOk;

  return MqttOk;
}

bool MqttClient::isSubscribedTo(const Topic &topic) const
{
  for (const auto &subscription : subscriptions_)
    if (subscription.matches(topic))
      return true;
  return false;
}

/* Setters for auth user/password */
void MqttBroker::SetAuthUser(const char *user)
{
  if (authUser_ && authUser_ != "")
  {
    free((void *)authUser_);
  }
  authUser_ = user ? strdup(user) : "";
}

void MqttBroker::SetAuthPassword(const char *password)
{
  if (authPassword_ && authPassword_ != "")
  {
    free((void *)authPassword_);
  }
  authPassword_ = password ? strdup(password) : "";
}

uint16_t MqttBroker::GetNextPacketId()
{
  if (nextPacketId_ == 0)
    nextPacketId_ = 1;
  return nextPacketId_++;
}
