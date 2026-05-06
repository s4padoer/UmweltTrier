#!/bin/bash
python3 -m pip install uv
python3 -m uv pip install . --target /build

ls -la /build | grep "dash"