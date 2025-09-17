# Load environment variables from .env if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

PROJECT_NAME := nrtsearch-ingestion-plugins
ARTIFACT_VERSION := $(shell grep "version = " build.gradle | cut -d"'" -f2)
KAFKA_ARTIFACT_FILE_PATH := kafka-plugin/build/distributions/kafka-plugin-$(ARTIFACT_VERSION).zip
PAIMON_ARTIFACT_FILE_PATH := paimon-plugin/build/distributions/paimon-plugin-$(ARTIFACT_VERSION).zip

# AWS variables (loaded from .env)
# AWS_ROLE := (from .env)
# S3_DEV_BUCKET := (from .env)
# S3_PNW_BUCKET := (from .env)
# S3_NOVA_BUCKET := (from .env)

# Java configuration (with fallback)
DEFAULT_JAVA := /opt/homebrew/Cellar/openjdk@21/21.0.6/libexec/openjdk.jdk/Contents/Home
ifneq ($(JAVA_HOME_OVERRIDE),)
    export JAVA_HOME := $(JAVA_HOME_OVERRIDE)
else ifneq ($(wildcard $(DEFAULT_JAVA)),)
    export JAVA_HOME := $(DEFAULT_JAVA)
endif

.ONESHELL:
.PHONY: assemble build clean

clean:
	JAVA_HOME="$(JAVA_HOME)" ./gradlew clean

assemble:
	JAVA_HOME="$(JAVA_HOME)" ./gradlew assemble

build:
	JAVA_HOME="$(JAVA_HOME)" ./gradlew build

test:
	JAVA_HOME="$(JAVA_HOME)" ./gradlew test

e2etest:
	JAVA_HOME="$(JAVA_HOME)" ./gradlew e2eTest

# Build distribution zips
dist:
	@echo "Building plugin distributions..."
	JAVA_HOME="$(JAVA_HOME)" ./gradlew clean distZip
	@echo "Created artifacts:"
	@echo "  - $(KAFKA_ARTIFACT_FILE_PATH)"
	@echo "  - $(PAIMON_ARTIFACT_FILE_PATH)"

# Upload to dev only (for development)
upload-dev: dist
	@if [ -z "$(S3_DEV_BUCKET)" ]; then \
		echo "Error: S3_DEV_BUCKET not set. Please check your .env file."; \
		exit 1; \
	fi
	@echo "Uploading to dev bucket: $(S3_DEV_BUCKET)"
	aws s3 cp $(KAFKA_ARTIFACT_FILE_PATH) $(S3_DEV_BUCKET) || exit $$?
	aws s3 cp $(PAIMON_ARTIFACT_FILE_PATH) $(S3_DEV_BUCKET) || exit $$?
	@echo "✅ Successfully uploaded to dev environment:"
	@echo "  - kafka-plugin-$(ARTIFACT_VERSION).zip"
	@echo "  - paimon-plugin-$(ARTIFACT_VERSION).zip"

# Full publish (dev + prod with role assumption)
publish: dist
	@if [ -z "$(S3_DEV_BUCKET)" ] || [ -z "$(AWS_ROLE)" ] || [ -z "$(S3_PNW_BUCKET)" ] || [ -z "$(S3_NOVA_BUCKET)" ]; then \
		echo "Error: Required AWS variables not set. Please check your .env file."; \
		echo "Required: S3_DEV_BUCKET, AWS_ROLE, S3_PNW_BUCKET, S3_NOVA_BUCKET"; \
		exit 1; \
	fi

	@echo "Uploading to dev..."
	aws s3 cp $(KAFKA_ARTIFACT_FILE_PATH) $(S3_DEV_BUCKET) || exit $$?
	aws s3 cp $(PAIMON_ARTIFACT_FILE_PATH) $(S3_DEV_BUCKET) || exit $$?

	@echo "Assuming role for prod upload..."
	sts_json=$$(aws sts assume-role --role-arn $(AWS_ROLE) --role-session-name nrtsearch-ingestion-plugins-deploy | jq .Credentials)
	export AWS_ACCESS_KEY_ID=$$(echo "$$sts_json" | jq -r .AccessKeyId)
	export AWS_SECRET_ACCESS_KEY=$$(echo "$$sts_json" | jq -r .SecretAccessKey)
	export AWS_SESSION_TOKEN=$$(echo "$$sts_json" | jq -r .SessionToken)

	@echo "Uploading to prod buckets..."
	aws s3 cp $(KAFKA_ARTIFACT_FILE_PATH) $(S3_PNW_BUCKET) || exit $$?
	aws s3 cp $(KAFKA_ARTIFACT_FILE_PATH) $(S3_NOVA_BUCKET) || exit $$?
	aws s3 cp $(PAIMON_ARTIFACT_FILE_PATH) $(S3_PNW_BUCKET) || exit $$?
	aws s3 cp $(PAIMON_ARTIFACT_FILE_PATH) $(S3_NOVA_BUCKET) || exit $$?

	@echo "✅ Successfully published to all environments:"
	@echo "  - kafka-plugin-$(ARTIFACT_VERSION).zip"
	@echo "  - paimon-plugin-$(ARTIFACT_VERSION).zip"

# Check environment configuration
env-check:
	@echo "Environment Configuration:"
	@echo "  JAVA_HOME: $(JAVA_HOME)"
	@echo "  Project Version: $(ARTIFACT_VERSION)"
	@echo "  S3_DEV_BUCKET: $(S3_DEV_BUCKET)"
	@echo "  AWS_ROLE: $(AWS_ROLE)"
	@echo "  S3_PNW_BUCKET: $(S3_PNW_BUCKET)"
	@echo "  S3_NOVA_BUCKET: $(S3_NOVA_BUCKET)"

# Passes through any other commands to gradle
%:
	JAVA_HOME="$(JAVA_HOME)" ./gradlew "$@"