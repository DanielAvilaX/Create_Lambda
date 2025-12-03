# Makefile dentro de src/

PYTHON_VERSION := python3.12
PACKAGE_NAME := ../lambda_package.zip
SRC_DIR := .
TEMP_DIR := ../build_temp

default: clean package

install-deps:
	powershell -Command "if (Test-Path '$(TEMP_DIR)') { Remove-Item '$(TEMP_DIR)' -Recurse -Force }"
	powershell -Command "New-Item -ItemType Directory -Path '$(TEMP_DIR)' | Out-Null"
	docker run --rm --entrypoint "" -v "$(CURDIR)":/var/task public.ecr.aws/lambda/python:3.12 pip install -r /var/task/requirements.txt -t /var/task

package: install-deps
	powershell -Command "if (Test-Path '$(PACKAGE_NAME)') { Remove-Item '$(PACKAGE_NAME)' -Force }"
	powershell -Command "Compress-Archive -Path '$(SRC_DIR)/*' -DestinationPath '$(PACKAGE_NAME)' -Force"

clean:
	powershell -Command "if (Test-Path '$(PACKAGE_NAME)') { Remove-Item '$(PACKAGE_NAME)' -Force }"
	powershell -Command "if (Test-Path '$(TEMP_DIR)') { Remove-Item '$(TEMP_DIR)' -Recurse -Force }"

.PHONY: default install-deps package clean
