import unittest
from transcript import coalesce, page
from trigger_adapter import TriggerAdapter
from server import validate_session_input


class CommandProjectionTests(unittest.TestCase):
    def test_latest_commands_are_available_even_when_transcript_is_paged(self):
        events = [{'type': 'commands', 'commands': [{'name': 'old'}]}]
        events += [{'type': 'user', 'text': str(i)} for i in range(150)]
        events += [{'type': 'commands', 'commands': [{'name': 'new'}]}]
        rows = coalesce(events)
        newest, more = page(rows, None, 100)
        self.assertTrue(more)
        self.assertEqual(newest[0]['key'], 'commands')
        self.assertEqual(newest[0]['commands'], [{'name': 'new'}])
        self.assertEqual(sum(row['type'] == 'commands' for row in rows), 1)

    def test_trigger_command_metadata_survives_trimmed_events_and_can_clear(self):
        metadata = {'eventsDropped': 300, 'events': [{'type': 'user', 'text': 'recent'}],
                    'commands': [{'name': 'review'}]}
        self.assertEqual(coalesce(TriggerAdapter._events_with_commands(metadata))[-1]['text'], 'recent')
        self.assertEqual(coalesce(TriggerAdapter._events_with_commands(metadata))[0]['commands'], [{'name': 'review'}])
        metadata['commands'] = []
        self.assertEqual(coalesce(TriggerAdapter._events_with_commands(metadata))[0]['commands'], [])

    def test_latest_acp_state_is_in_newest_page(self):
        events = [{'type': 'acp_state', 'usage': {'used': 5, 'size': 10}}]
        events += [{'type': 'user', 'text': str(i)} for i in range(150)]
        events += [{'type': 'acp_state', 'usage': {'used': 6, 'size': 10}}]
        newest, _ = page(coalesce(events), None, 20)
        self.assertEqual(next(row for row in newest if row['type'] == 'acp_state')['usage']['used'], 6)

    def test_attachment_and_setting_validation(self):
        message = {'type': 'message', 'text': '', 'attachments': [{'name': 'note.txt', 'mimeType': 'text/plain', 'data': 'aGk='}]}
        self.assertEqual(validate_session_input(message), message)
        self.assertEqual(validate_session_input({'type': 'config', 'id': 'permission', 'value': False})['value'], False)
        with self.assertRaises(ValueError):
            validate_session_input({'type': 'message', 'text': '', 'attachments': [{'name': '../note', 'mimeType': 'text/plain', 'data': 'aGk='}]})
