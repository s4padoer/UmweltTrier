#!/bin/bash
python3 -m pip install uv
uv pip install -e .

ls -la /build | grep "dash"