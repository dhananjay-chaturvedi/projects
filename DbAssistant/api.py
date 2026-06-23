"""Shim — expose the full DbManagementTool FastAPI app.

Three equivalent ways to start the server:

    python dbtool.py api [--host H] [--port P] [--reload]   # recommended
    python api.py        [--host H] [--port P] [--reload]   # this shim
    uvicorn api:app                                          # raw uvicorn

The actual implementation lives in :mod:`app.headless.api`; importing it here
gives ``api:app`` as the importable ASGI target alongside the existing
``app.headless.api:app`` so external runners (uvicorn, gunicorn, hypercorn,
Docker, systemd) can use either path.
"""

from app.headless.api import MOUNTED_MODULES, app

__all__ = ["app", "MOUNTED_MODULES", "main"]


def main() -> None:
    """Console entry point for ``dbassistant-api`` (``pip install dbassistant``)."""
    raise SystemExit(_serve())


def _serve() -> int:
    """Run the API server when this file is executed directly."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="api.py",
        description="Start the DbManagementTool REST API "
                    "(equivalent to: python dbtool.py api).",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true",
                        help="Auto-reload on code changes (dev mode)")
    args = parser.parse_args()

    try:
        import uvicorn
    except ImportError:
        print("[ERR] uvicorn not installed. Run: pip install uvicorn fastapi",
              file=sys.stderr)
        return 1

    print(f"[OK]  Starting REST API on http://{args.host}:{args.port}")
    print(f"[   ] Interactive docs : http://{args.host}:{args.port}/docs")
    print(f"[   ] Health check     : http://{args.host}:{args.port}/api/health")
    print(f"[   ] Mounted modules  : {', '.join(MOUNTED_MODULES) or 'none'}")

    # Use the import-string form so ``--reload`` works (reload requires a
    # string target, not an app instance).
    uvicorn.run(
        "api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
    return 0


if __name__ == "__main__":
    main()
