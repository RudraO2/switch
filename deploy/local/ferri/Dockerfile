# Ferri's Switch: the published core image with this fork's Python source on top.
# Built from the same commit the base image was, plus the fork's changes (agent
# message edits). How to build and run it: compose.override.yml.
ARG BASE
FROM ${BASE}
COPY core/switch_core/ /app/switch_core/
