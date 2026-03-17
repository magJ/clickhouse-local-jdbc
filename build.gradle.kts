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

val junitVersion = "5.12.2"

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter(junitVersion)
            dependencies {
                implementation("org.mockito:mockito-core:5.18.0")
            }
        }

        val integrationTest by registering(JvmTestSuite::class) {
            useJUnitJupiter(junitVersion)
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
