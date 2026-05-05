def test_service_entrypoints_import() -> None:
    import order_platform.services.api_gateway.app  # noqa: F401
    import order_platform.services.dlq_monitor.worker  # noqa: F401
    import order_platform.services.inventory_service.worker  # noqa: F401
    import order_platform.services.order_service.worker  # noqa: F401
    import order_platform.services.payment_service.worker  # noqa: F401
    import order_platform.services.saga_orchestrator.worker  # noqa: F401
    import order_platform.services.shipping_service.worker  # noqa: F401
