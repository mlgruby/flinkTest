package org.flink.assignment.config

import java.util.Properties

/**
  * ConfigManager is a utility object for managing configuration properties.
  * It supports loading properties from a file and retrieving them with optional
  * fallback to environment variables.
  */
object ConfigManager {
  private val properties = new Properties()

  /**
    * Loads configuration from a specified file.
    *
    * @param configFile The name of the configuration file to load.
    * @throws RuntimeException if the configuration file is not found.
    */
  def loadConfig(configFile: String): Unit = {
    val inputStream = getClass.getResourceAsStream(s"/$configFile")
    if (inputStream != null) {
      try {
        properties.load(inputStream)
      } finally {
        inputStream.close()
      }
    } else {
      throw new RuntimeException(s"Configuration file $configFile not found")
    }
  }

  /**
    * Retrieves a property value by key.
    * First checks environment variables, then falls back to the loaded properties.
    *
    * @param key The key of the property to retrieve.
    * @return The value of the property.
    * @throws RuntimeException if the property is not found in either the environment or the loaded properties.
    */
  def getProperty(key: String): String = {
    // First, try to get the property from environment variables
    sys.env.get(key) match {
      case Some(value) => value
      case None        =>
        // If not found in environment, try to get from properties file
        Option(properties.getProperty(key)) match {
          case Some(value) => value
          case None        => throw new RuntimeException(s"Property $key not found in configuration or environment")
        }
    }
  }
}
