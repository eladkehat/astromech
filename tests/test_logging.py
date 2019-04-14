import logging


def test_logger(caplog, monkeypatch):
    log_message = 'Test message'
    monkeypatch.setenv('LOG_LEVEL', 'WARNING')
    from astromech.logging import logger
    assert logger.getEffectiveLevel() == logging.WARNING
    with caplog.at_level(logging.WARNING):
        logger.warning(log_message)
        assert log_message in caplog.text
