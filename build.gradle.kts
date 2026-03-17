plugins {
    java
}

group = "io.github.magj"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter("5.10.1")
            dependencies {
                implementation("org.mockito:mockito-core:5.8.0")
            }
        }

        val integrationTest by registering(JvmTestSuite::class) {
            useJUnitJupiter("5.10.1")
            dependencies {
                implementation(project())
            }
            targets {
                all {
                    testTask.configure {
                        shouldRunAfter(tasks.named("test"))
                        systemProperty(
                            "clickhouseLocalPath",
                            System.getProperty("clickhouseLocalPath", "clickhouse-local")
                        )
                    }
                }
            }
        }
    }
}
