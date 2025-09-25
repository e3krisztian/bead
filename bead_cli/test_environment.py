from .environment import Environment


def test_load_legacy_environment_defaults_to_enabled(robot):
    # setup a pre-enable/disable-era environment
    env_path = robot.config_dir / 'boxes.json'
    env_path.write_text('''
        {
            "boxes": [
                {
                    "name": "box1",
                    "directory": "/tmp/box1"
                },
                {
                    "name": "box2",
                    "directory": "/tmp/box2"
                }
            ]
        }
    ''')
    env = Environment(robot.config_dir, robot.state_dir)
    boxes = env.get_all_boxes()
    assert len(boxes) == 2
    assert boxes[0].enabled
    assert boxes[1].enabled

    assert len(env.get_boxes()) == 2


def test_disable_and_enable_box(robot):
    env = Environment(robot.config_dir, robot.state_dir)
    # robot fixture creates a 'box'
    assert env.get_box('box').enabled
    assert len(env.get_boxes()) == 1

    env.disable_box('box')
    assert not env.get_box('box').enabled
    assert len(env.get_boxes()) == 0
    all_boxes = env.get_all_boxes()
    assert len(all_boxes) == 1
    assert not all_boxes[0].enabled

    env.enable_box('box')
    assert env.get_box('box').enabled
    assert len(env.get_boxes()) == 1


def test_persistence_of_disabled_state(robot):
    env = Environment(robot.config_dir, robot.state_dir)
    env.disable_box('box')
    env.save()

    # create a new environment to see if it loads the saved state
    new_env = Environment(robot.config_dir, robot.state_dir)
    assert not new_env.get_box('box').enabled
    assert len(new_env.get_boxes()) == 0
