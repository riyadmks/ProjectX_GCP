#!/bin/bash
#comment
steps:
- name: gcr.io/cloud-builders/gsutil
  args: ['cp', '/workspace', 'gs://projet_smart_gcp/Github']
