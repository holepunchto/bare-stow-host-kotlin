import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
  alias(libs.plugins.kotlin.multiplatform)
  alias(libs.plugins.android.kotlin.multiplatform.library)
  alias(libs.plugins.dokka)
  alias(libs.plugins.ktfmt)
  id("maven-publish")
}

group = "com.github.holepunchto"

version = System.getenv("VERSION") ?: "0.1.1"

kotlin {
  explicitApi()

  @OptIn(org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation::class) abiValidation()

  compilerOptions { allWarningsAsErrors = true }

  jvm { compilerOptions { jvmTarget = JvmTarget.JVM_11 } }

  android {
    namespace = "to.holepunch.bare.stow.host"
    compileSdk = 36
    minSdk = 29
    withHostTest {}
    compilerOptions { jvmTarget = JvmTarget.JVM_11 }
  }

  sourceSets {
    commonMain.dependencies {
      api(libs.kotlinx.coroutines.core)
      implementation(libs.kotlinx.serialization.json)
    }
    commonTest.dependencies { implementation(kotlin("test")) }
  }
}

dokka {
  dokkaSourceSets.configureEach { reportUndocumented = true }
  dokkaPublications.html { failOnWarning = true }
}

ktfmt { googleStyle() }

tasks.named<Test>("jvmTest") { useJUnitPlatform() }

tasks.register("lint") {
  group = "verification"
  description = "Checks the formatting, the ABI, and the documentation."
  dependsOn("ktfmtCheck", "checkKotlinAbi", "dokkaGenerate")
}

tasks.register("test") {
  group = "verification"
  description = "Runs the tests on every target."
  dependsOn("jvmTest", "testAndroidHostTest")
}

tasks.check { dependsOn("lint") }
