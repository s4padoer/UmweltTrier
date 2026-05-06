#!/bin/bash
python3 -m pip install uv
python3 -m uv pip install -e .

ls -la /build | grep "dash"