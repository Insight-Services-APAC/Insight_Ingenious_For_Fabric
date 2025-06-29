from unittest import mock

from ingen_fab.notebook_utils.simple_notebook import SimpleFabricNotebook


def test_create_run_job():
    cred = mock.Mock()
    cred.get_token.return_value.token = "token"

    nb = SimpleFabricNotebook("ws", credential=cred)

    with mock.patch("requests.post") as post, mock.patch("requests.get") as get:
        # Mock create notebook response
        post.return_value.status_code = 201
        post.return_value.json.return_value = {"id": "nb1"}
        post.return_value.headers = {"Location": "https://api/nb/jobs/job1"}

        # Mock run notebook response for second call
        def post_side_effect(*args, **kwargs):
            if "jobs" in args[0]:
                resp = mock.Mock(status_code=202)
                resp.headers = {"Location": "https://api/nb/jobs/job1"}
                resp.json.return_value = {}
                return resp
            return post.return_value

        post.side_effect = post_side_effect

        # Mock status polling
        get.return_value.status_code = 200
        get.return_value.json.return_value = {"status": "Succeeded"}

        status = nb.run_and_wait("demo", "print('x')", poll_interval=0, timeout=1)

        assert status == "Succeeded"
