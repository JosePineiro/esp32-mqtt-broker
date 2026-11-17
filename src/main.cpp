/**
 * @file main.cpp
 * @brief ESP32 MQTT Broker using TinyMqtt with robust WiFi management and ESP-IDF logging.
 * @author Jose M. Piñeiro
 * @date 2026
 */

#include <Arduino.h>
#include "TinyMqttServer.h"
#include <vector>
#include <string>
#include <esp_log.h>

/// MQTT broker port configuration
const uint16_t PORT = 1883;
/// WiFi credentials
const char *ssid = "DIGIFIBRA-AChU";
const char *password = "N7s9dbtH`.*T";

/// MQTT broker instance allowing up to 100 concurrent clients
MqttBroker broker(PORT, 100);

/**
 * @brief Converts a WiFi status code into a human-readable string.
 * @param s The wl_status_t enumeration value representing current WiFi state.
 * @return const char* A descriptive string of the WiFi status.
 */
static const char *wifiStatusName(wl_status_t s)
{
	switch (s)
	{
	case WL_IDLE_STATUS:
		return "IDLE";
	case WL_NO_SSID_AVAIL:
		return "NO_SSID";
	case WL_SCAN_COMPLETED:
		return "SCAN_COMPLETED";
	case WL_CONNECTED:
		return "CONNECTED";
	case WL_CONNECT_FAILED:
		return "CONNECT_FAILED";
	case WL_CONNECTION_LOST:
		return "CONNECTION_LOST";
	case WL_DISCONNECTED:
		return "DISCONNECTED";
	default:
		return "UNKNOWN";
	}
}

/**
 * @brief Manages WiFi connection with a blocking retry loop and detailed diagnostics.
 * @details Disables persistent storage and auto-reconnect, attempts connection for up to 30 seconds,
 *          and performs a network scan for troubleshooting if connection fails.
 */
void connectToWiFi()
{
	const unsigned long TIMEOUT_MS = 30000;

	WiFi.persistent(false);
	WiFi.mode(WIFI_STA);
	WiFi.setAutoReconnect(false);

	while (true)
	{
		log_d("[WiFi] Connecting to '%s'...", ssid);
		WiFi.disconnect(true, false);
		delay(100);
		WiFi.begin(ssid, password);

		unsigned long startTime = millis();
		unsigned long lastDot = 0;
		wl_status_t lastSt = WL_IDLE_STATUS;

		while (millis() - startTime < TIMEOUT_MS)
		{
			lastSt = WiFi.status();
			if (lastSt == WL_CONNECTED)
			{
				log_d("[WiFi] CONNECTED. IP=%s  RSSI=%d dBm", WiFi.localIP().toString().c_str(), WiFi.RSSI());
				return;
			}

			if (millis() - lastDot >= 5000)
			{
				lastDot = millis();
				log_d("[WiFi]    ...%lu s, status=%s", (millis() - startTime) / 1000, wifiStatusName(lastSt));
			}
			delay(100);
		}

		// Failure diagnostics
		log_e("[WiFi] Timeout after %lu s. Last status: %d (%s)", TIMEOUT_MS / 1000, lastSt, wifiStatusName(lastSt));

		WiFi.disconnect(true, false);
		delay(150);

		int n = WiFi.scanNetworks();
		if (n <= 0)
		{
			log_e("[WiFi] No networks detected. Check antenna or router.");
		}
		else
		{
			bool found = false;
			int rssi = 0, enc = -1;
			for (int i = 0; i < n; i++)
			{
				if (WiFi.SSID(i) == String(ssid))
				{
					found = true;
					rssi = WiFi.RSSI(i);
					enc = (int)WiFi.encryptionType(i);
					break;
				}
			}

			if (!found)
				log_e("[WiFi] '%s' NOT found among %d visible networks (hidden or out of range).", ssid, n);
			else
				log_e("[WiFi] '%s' found (RSSI %d dBm, enc=%d) but association failed (wrong password/WPA3).", ssid, rssi, enc);
		}
		WiFi.scanDelete();
		log_d("[WiFi] Retrying...");
	}
}

/**
 * @brief Displays connected MQTT clients in a formatted table and logs memory usage.
 * @details Tracks previous states to update the table only when client data changes.
 *          Uses ESP-IDF logging macros instead of Serial.
 */
void display()
{
	static struct ClientData
	{
		std::vector<std::string> ids;
		std::vector<bool> connected;
		std::vector<int> keepAlive;
		bool operator!=(const ClientData &o) const
		{
			return ids != o.ids || connected != o.connected || keepAlive != o.keepAlive;
		}
	} lastState;

	ClientData currentState;
	for (auto client : broker.GetClients())
	{
		currentState.ids.push_back(client->id());
		currentState.connected.push_back(client->connected());
		currentState.keepAlive.push_back(client->keepAlive());
	}

	if (currentState != lastState)
	{
		lastState = currentState;

		std::string table = "\n---------------------------------------------------\n";
		table += "| ID                     | Connected | Keep Alive |\n";
		table += "---------------------------------------------------\n";

		char row[128];
		for (size_t i = 0; i < currentState.ids.size(); i++)
		{
			snprintf(row, sizeof(row), "| %-22s | %-9s | %-10d |\n",
					 currentState.ids[i].c_str(),
					 currentState.connected[i] ? "Yes" : "No",
					 currentState.keepAlive[i]);
			table += row;
		}
		table += "---------------------------------------------------";

		log_i("%s", table.c_str());
		log_i("[MQTT] Free PSRAM: %d bytes | Free SRAM: %d bytes", ESP.getFreePsram(), ESP.getFreeHeap());
	}
}

/**
 * @brief Arduino setup routine.
 * @details Sets global log levels, initiates WiFi connection, starts the MQTT broker,
 *          and configures authentication credentials.
 */
void setup()
{
	esp_log_level_set("*", ESP_LOG_DEBUG);

	connectToWiFi();

	broker.Start();
	broker.SetAuthUser("test");
	broker.SetAuthPassword("1234");
	log_i("Broker ready: %s on port %d", WiFi.localIP().toString().c_str(), PORT);
}

/**
 * @brief Arduino main loop routine.
 * @details Continuously checks WiFi connectivity, handles MQTT broker network tasks,
 *          and conditionally prints client diagnostic tables based on debug levels.
 */
void loop()
{
	if (WiFi.status() != WL_CONNECTED)
	{
		connectToWiFi();
	}

	broker.Poll();

#if CORE_DEBUG_LEVEL > 2
	display();
#endif
}