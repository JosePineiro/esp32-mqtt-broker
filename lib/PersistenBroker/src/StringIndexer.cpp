#include "StringIndexer.h"
#include <cstring>

// Inicialización estática
StringIndexer::StringMap StringIndexer::strings;

// ============================================================================
// StringIndexer - Métodos privados
// ============================================================================

uint32_t StringIndexer::fnv1a_hash(const char *str, uint8_t len)
{
  uint32_t hash = 2166136261U;
  for (uint8_t i = 0; i < len; i++)
  {
    hash ^= static_cast<uint8_t>(str[i]);
    hash *= 16777619U;
  }
  return hash;
}

uint8_t StringIndexer::strToIndex(const char *str, uint8_t len)
{
  if (len == 0)
    return 0;

  uint32_t hash = fnv1a_hash(str, len);

  // Búsqueda: primero por hash, luego verificar contenido
  for (auto &p : strings)
  {
    StringEntry &entry = p.second;
    if (entry.str.length() == len && entry.hash == hash && 
        memcmp(entry.str.c_str(), str, len) == 0)
    {
      entry.used++;
      return p.first;
    }
  }

  // Crear nuevo índice
  for (uint8_t idx = 1; idx != 0; idx++)
  {
    if (strings.find(idx) == strings.end())
    {
      StringEntry &entry = strings[idx];
      entry.str.assign(str, len);
      entry.hash = hash;
      entry.used = 1;
      return idx;
    }
  }
  return 0; // Sin índices disponibles
}

// ============================================================================
// StringIndexer - Métodos públicos
// ============================================================================

const std::string &StringIndexer::str(uint8_t index)
{
  static const std::string dummy;
  auto it = strings.find(index);
  return (it != strings.end()) ? it->second.str : dummy;
}

void StringIndexer::use(uint8_t index)
{
  auto it = strings.find(index);
  if (it != strings.end())
    it->second.used++;
}

void StringIndexer::release(uint8_t index)
{
  auto it = strings.find(index);
  if (it != strings.end())
  {
    if (--it->second.used == 0)
    {
      strings.erase(it);
    }
  }
}

uint16_t StringIndexer::count()
{
  return strings.size();
}

// ============================================================================
// IndexedString - Constructores y Destructores
// ============================================================================

IndexedString::IndexedString() : index(0)
{
}

IndexedString::IndexedString(const IndexedString &source) : index(source.index)
{
  StringIndexer::use(index);
}

IndexedString::IndexedString(IndexedString &&src) noexcept : index(src.index)
{
  src.index = 0;
}

IndexedString::IndexedString(const char *str, uint8_t len)
{
  index = StringIndexer::strToIndex(str, len);
}

IndexedString::IndexedString(const std::string &str) 
  : IndexedString(str.c_str(), str.length())
{
}

IndexedString::~IndexedString()
{
  if (index != 0)
    StringIndexer::release(index);
}

// ============================================================================
// IndexedString - Operadores de asignación
// ============================================================================

IndexedString &IndexedString::operator=(const IndexedString &source)
{
  if (this != &source)
  {
    if (index != 0)
      StringIndexer::release(index);
    index = source.index;
    StringIndexer::use(index);
  }
  return *this;
}

IndexedString &IndexedString::operator=(IndexedString &&src) noexcept
{
  if (this != &src)
  {
    if (index != 0)
      StringIndexer::release(index);
    index = src.index;
    src.index = 0;
  }
  return *this;
}

// ============================================================================
// IndexedString - Operadores de comparación
// ============================================================================

bool operator<(const IndexedString &i1, const IndexedString &i2)
{
  return i1.index < i2.index;
}

bool operator==(const IndexedString &i1, const IndexedString &i2)
{
  return i1.index == i2.index;
}

bool operator!=(const IndexedString &i1, const IndexedString &i2)
{
  return i1.index != i2.index;
}

// ============================================================================
// IndexedString - Métodos
// ============================================================================

const std::string &IndexedString::str() const
{
  return StringIndexer::str(index);
}

uint8_t IndexedString::getIndex() const
{
  return index;
}

bool IndexedString::empty() const
{
  return index == 0;
}