#pragma once
#include <unordered_map>
#include <string>
#include <cstdint>

// ToDo: https://www.incibe.es/incibe-cert/alerta-temprana/vulnerabilidades/cve-2025-56353
/***
 * Optimizado para ESP32-S3: almacena hasta 255 cadenas con índice de 1 byte
 * Velocidad: búsqueda O(1) con hash de contenido
 * Memoria: pool estático con referencia counting
 */
class StringIndexer
{
private:
  struct StringEntry
  {
    std::string str;
    uint8_t used = 0;
    uint32_t hash = 0;
  };

  using StringMap = std::unordered_map<uint8_t, StringEntry>;
  static StringMap strings;

  // Hash FNV-1a para rápida comparación
  static uint32_t fnv1a_hash(const char *str, uint8_t len);

public:
  static const std::string &str(uint8_t index);
  static void use(uint8_t index);
  static void release(uint8_t index);
  static uint16_t count();

private:
  friend class IndexedString;
  static uint8_t strToIndex(const char *str, uint8_t len);
};

class IndexedString
{
public:
  IndexedString();
  IndexedString(const IndexedString &source);
  IndexedString(IndexedString &&src) noexcept;
  IndexedString(const char *str, uint8_t len);
  explicit IndexedString(const std::string &str);
  ~IndexedString();

  IndexedString &operator=(const IndexedString &source);
  IndexedString &operator=(IndexedString &&src) noexcept;

  friend bool operator<(const IndexedString &i1, const IndexedString &i2);
  friend bool operator==(const IndexedString &i1, const IndexedString &i2);
  friend bool operator!=(const IndexedString &i1, const IndexedString &i2);

  const std::string &str() const;
  uint8_t getIndex() const;
  bool empty() const;

private:
  uint8_t index = 0;
};