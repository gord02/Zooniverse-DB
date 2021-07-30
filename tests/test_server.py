
TEST_EVENT = {
    "event": 131000000,
    "dm": 123.4,
    "beams": {
       "snr": 7.9,
        "beam" : 1123,
        "data_paths": {
            123: "path to b1",
            1123: "path to b2",
        },
    },
    "transfer_status": "INCOMPLETE",
    "zooniverse_classification": "INCOMPLETE",
    "expert_classification": "INCOMPLETE",
}


async def test_get_all_events(test_cli):
    resp = await test_cli.get("/zooniverse-db/all-events")
    assert resp.status == 200

async def test_add_event(test_cli):
    resp = await test_cli.post("/zooniverse-db/transfer-event", json=TEST_EVENT)
    assert resp.status == 200

async def test_update_event(test_cli):
    update = {"dm": 1212.4}
    resp = await test_cli.put("/zooniverse-db/event/{}".format(TEST_EVENT["event"]), json=update)
    assert resp.status == 200

async def test_get_event(test_cli):
    resp = await test_cli.get("/zooniverse-db/event/{}".format(TEST_EVENT["event"]))
    assert resp.status == 200

async def test_zooniverse_classification(test_cli):
    resp = await test_cli.put(
        "/zooniverse-db/zooniverse-classification/{}/{}".format(TEST_EVENT["event"], "GOOD")
    )
    assert resp.status == 200

async def test_expert_classification(test_cli):
    resp = await test_cli.put(
        "/zooniverse-db/expert-classification/{}/{}".format(TEST_EVENT["event"], "GOOD")
    )
    assert resp.status == 200

async def test_fetch_events_for_transfer(test_cli):
    resp = await test_cli.get("/zooniverse-db/events-for-transfer")
    assert resp.status == 200

async def test_fetch_events_for_clenup(test_cli):
    resp = await test_cli.get("/zooniverse-db/events-for-cleanup")
    assert resp.status == 200

async def test_delete_event(test_cli):
    resp = await test_cli.delete("/zooniverse-db/event/{}".format(TEST_EVENT["event"]))
    assert resp.status == 200

async def test_fetch_events_for_experts(test_cli):
    resp = await test_cli.get("/zooniverse-db/events-for-experts")
    assert resp.status == 200


