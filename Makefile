# Note that this currently won't work if you don't have all the proper libraries installed.
$(eval SCRIPT_VERSION=$(shell python -c 'from __future__ import print_function; from athena_glue_service_logs import version; print(version.__version__)'))

# Local assets
PACKAGE_FILE=dist/athena_glue_converter_$(SCRIPT_VERSION).zip
JOB_DEFINITION_FILE=scripts/glue_jobs.json

# S3 destinations for releases
RELEASE_BUCKET?=aws-emr-bda-public
RELEASE_PREFIX=agsl/glue_scripts
RELEASE_S3_LOCATION=s3://$(RELEASE_BUCKET)/$(RELEASE_PREFIX)
RELEASE_PATH=$(RELEASE_S3_LOCATION)/athena_glue_converter_$(SCRIPT_VERSION).zip
RELEASE_LATEST_PATH=$(RELEASE_S3_LOCATION)/athena_glue_converter_latest.zip

RELEASE_PATH_KEY=$(shell echo $(RELEASE_PATH) | cut -f4- -d/)
RELEASE_LATEST_PATH_KEY=$(shell echo $(RELEASE_LATEST_PATH) | cut -f4- -d/)

# Temporary directory used for writing out Glue state files
TEMP_PREFIX=tmp/glue
GLUE_TEMP_S3_LOCATION?=s3://$(RELEASE_BUCKET)/$(TEMP_PREFIX)

.PHONY: test

next_version:
	@grep __version__ athena_glue_service_logs/version.py | cut -f3 -d\  | tr -d "'" | tr -d 'v' | awk -F. '{$$NF+=1; OFS="."; print $$0}'

create_dist:
	@[ ! -d dist ] && mkdir dist || echo

package: create_dist
	zip $(PACKAGE_FILE) -r athena_glue_service_logs/* -x "*.pyc"

private_release: package
	aws s3 cp $(PACKAGE_FILE) $(RELEASE_PATH)
	aws s3 cp $(PACKAGE_FILE) $(RELEASE_LATEST_PATH)
	aws s3 sync --exclude "*" --include "*.py" scripts/ $(RELEASE_S3_LOCATION)/

release: package
	aws s3 cp $(PACKAGE_FILE) $(RELEASE_PATH)
	aws s3api put-object-acl --acl public-read --bucket $(RELEASE_BUCKET) --key $(RELEASE_PATH_KEY)
	aws s3 cp $(PACKAGE_FILE) $(RELEASE_LATEST_PATH)
	aws s3api put-object-acl --acl public-read --bucket $(RELEASE_BUCKET) --key $(RELEASE_LATEST_PATH_KEY)
	aws s3 sync --exclude "*" --include "*.py" scripts/ $(RELEASE_S3_LOCATION)/
	for file in scripts/*.py; do \
		aws s3api put-object-acl --acl public-read --bucket $(RELEASE_BUCKET) --key $(RELEASE_PREFIX)/`basename $$file`; \
	done

# Since we want to be able to test multiple service types, let's make something more dynamic.
# For now, this requires the `jq` utility
define get_variable
$(shell cat $(JOB_DEFINITION_FILE) | jq '[.defaults,.$(service)] | add' | jq -r '.$(1)')
endef
JOB_NAME_BASE = $(call get_variable,JOB_NAME_BASE)
RAW_DATABASE_NAME = $(call get_variable,RAW_DATABASE_NAME)
RAW_TABLE_NAME = $(call get_variable,RAW_TABLE_NAME)
CONVERTED_DATABASE_NAME = $(call get_variable,CONVERTED_DATABASE_NAME)
CONVERTED_TABLE_NAME = $(call get_variable,CONVERTED_TABLE_NAME)
S3_CONVERTED_TARGET = $(call get_variable,S3_CONVERTED_TARGET)
S3_SOURCE_LOCATION = $(call get_variable,S3_SOURCE_LOCATION)

require_service:
	@test -n "$(service)" || (echo "'service' variable must be defined" && exit 1)

create_job: require_service
	$(eval SAMPLE_SCRIPT_PATH=$(RELEASE_S3_LOCATION)/sample_$(service)_job.py)
	test -n $(JOB_NAME_BASE) || (echo "$(service) service not found" && echo 1)
	aws glue create-job --name $(JOB_NAME_BASE)_LogMaster_$(SCRIPT_VERSION) \
		--description "$(JOB_NAME_BASE) Log infra generator" \
		--role AWSGlueServiceRoleDefault \
		--command Name=glueetl,ScriptLocation=$(SAMPLE_SCRIPT_PATH) \
		--default-arguments '{ \
			"--extra-py-files":"$(RELEASE_LATEST_PATH)", \
			"--TempDir":"$(GLUE_TEMP_S3_LOCATION)", \
			"--job-bookmark-option":"job-bookmark-enable", \
			"--raw_database_name":"$(RAW_DATABASE_NAME)", \
			"--raw_table_name":"$(RAW_TABLE_NAME)", \
			"--converted_database_name":"$(CONVERTED_DATABASE_NAME)", \
			"--converted_table_name":"$(CONVERTED_TABLE_NAME)", \
			"--s3_converted_target":"$(S3_CONVERTED_TARGET)", \
			"--s3_source_location":"$(S3_SOURCE_LOCATION)" \
		}'

clean_glue: require_service
	aws glue delete-table --database-name $(RAW_DATABASE_NAME) --name $(RAW_TABLE_NAME)
	aws glue delete-table --database-name $(CONVERTED_DATABASE_NAME) --name $(CONVERTED_TABLE_NAME)
	aws glue delete-job --job-name $(JOB_NAME_BASE)_LogMaster_$(SCRIPT_VERSION)
	aws s3 rm --recursive $(S3_CONVERTED_TARGET)

test:
	python -m pytest test
