# flake8: noqa

def move_then_count():
    "Move motor1 and motor2 into position; then count det."
    yield from mv(motor1, 1, motor2, 10)
    yield from count([det1, det2])
