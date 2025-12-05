// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License (Version 2.0).
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "otel_process_ctx.h"

double burn_cpu(void) {
  double x = 0.0;
  for (int i = 0; i < 1000000; i++) {
    x = (x+i)*(x+i);
  }
  return x;
}

void burn_cpu_forever(void) {
  // avoid compiler optimization
  volatile double x = 0.0;
  while (true) {
    x += burn_cpu();
  }
}

void burn_cpu_for(int seconds) {
  // avoid compiler optimization
  volatile double x = 0.0;
  time_t t0 = time(NULL);
  while (time(NULL) - t0 < seconds) {
    x += burn_cpu();
  }
}

bool read_and_print_ctx(const char* prefix) {
  otel_process_ctx_read_result result = otel_process_ctx_read();

  if (!result.success) {
    fprintf(stderr, "Failed to read context: %s\n", result.error_message);
    return false;
  }

  printf(
    "%s (for pid %d): service=%s, instance=%s, env=%s, version=%s, sdk=%s/%s/%s",
    prefix,
    getpid(),
    result.data.service_name,
    result.data.service_instance_id,
    result.data.deployment_environment_name,
    result.data.service_version,
    result.data.telemetry_sdk_name,
    result.data.telemetry_sdk_language,
    result.data.telemetry_sdk_version
  );

  if (result.data.resources && result.data.resources[0] != NULL) {
    printf(", resources=");
    for (int i = 0; result.data.resources[i] != NULL; i += 2) {
      if (i > 0) printf(",");
      printf("%s=%s", result.data.resources[i], result.data.resources[i + 1]);
    }
  } else {
    printf(", resources=(none)");
  }
  printf("\n");

  otel_process_ctx_read_drop(&result);
  return true;
}

const char *resources[] = {
  "resource.key1", "resource.value1",
  "resource.key2", "resource.value2",
  NULL
};

int update_and_fork(void) {
  printf("Burning CPU for 5 seconds...\n");
  burn_cpu_for(5);
  printf("Updating...\n");

  otel_process_ctx_data update_data = {
    .deployment_environment_name = "staging",
    .service_instance_id = "456d8444-2c7e-46e3-89f6-6217880f7456",
    .service_name = "my-service-updated",
    .service_version = "7.8.9",
    .telemetry_sdk_language = "c",
    .telemetry_sdk_version = "1.2.3",
    .telemetry_sdk_name = "example_ctx.c",
    .resources = resources
  };

  otel_process_ctx_result result = otel_process_ctx_publish(&update_data);
  if (!result.success) {
    fprintf(stderr, "Failed to update: %s\n", result.error_message);
    return 1;
  }

  if (!read_and_print_ctx("Updated")) return 1;

  printf("Forking...\n");

  if (fork() == 0) {
    printf("[child] Calling update in child...\n");
    burn_cpu_for(5);

    otel_process_ctx_data child_data = {
      .deployment_environment_name = "staging",
      .service_instance_id = "789d8444-2c7e-46e3-89f6-6217880f7789",
      .service_name = "my-service-forked",
      .service_version = "10.11.12",
      .telemetry_sdk_language = "c",
      .telemetry_sdk_version = "1.2.3",
      .telemetry_sdk_name = "example_ctx.c",
      .resources = NULL
    };

    result = otel_process_ctx_publish(&child_data);
    if (!result.success) {
      fprintf(stderr, "[child] Failed to update: %s\n", result.error_message);
      return 1;
    }

    if (!read_and_print_ctx("[child] Updated")) return 1;

    burn_cpu_for(5);

    if (!otel_process_ctx_drop_current()) {
      fprintf(stderr, "[child] Failed to drop process context\n");
      return 1;
    }

    return 0;
  }

  wait(NULL);

  if (!otel_process_ctx_drop_current()) {
    fprintf(stderr, "Failed to drop process context\n");
    return 1;
  }

  return 0;
}


int main(int argc, char* argv[]) {
  bool keep_running = false;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--keep-running") == 0) {
      keep_running = true;
    } else {
      fprintf(stderr, "Unknown argument: %s\n", argv[i]);
      fprintf(stderr, "Usage: %s [--keep-running]\n", argv[0]);
      return 1;
    }
  }

  otel_process_ctx_data data = {
    .deployment_environment_name = "prod",
    .service_instance_id = "123d8444-2c7e-46e3-89f6-6217880f7123",
    .service_name = "my-service",
    .service_version = "4.5.6",
    .telemetry_sdk_language = "c",
    .telemetry_sdk_version = "1.2.3",
    .telemetry_sdk_name = "example_ctx.c",
    .resources = resources
  };

  otel_process_ctx_result result = otel_process_ctx_publish(&data);
  if (!result.success) {
    fprintf(stderr, "Failed to publish: %s\n", result.error_message);
    return 1;
  }

  if (!read_and_print_ctx("Published")) return 1;

  if (keep_running) {
    printf("Continuing forever, to exit press ctrl+c...\n");
    printf("TIP: You can now `sudo ./otel_process_ctx_dump.sh %d` to see the context\n", getpid());
    burn_cpu_forever();
  } else {
    return update_and_fork();
  }
}
