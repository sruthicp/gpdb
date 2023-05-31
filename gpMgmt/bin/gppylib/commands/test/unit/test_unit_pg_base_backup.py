#!/usr/bin/env python3
#
# Copyright (c) Greenplum Inc 2012. All Rights Reserved.
#

import unittest
from mock import call, Mock, patch, MagicMock
from gppylib.commands import pg
from pgdb import DatabaseError

from gppylib.test.unit.gp_unittest import GpTestCase
from gppylib.commands.base import CommandResult

class TestUnitPgReplicationSlot(GpTestCase):
    def setUp(self):
        mock_logger = Mock(spec=['log', 'warn', 'info', 'debug', 'error', 'warning', 'fatal'])
        self.replication_slot_name = "internal_wal_replication_slot"
        self.source_host = "bar"
        self.source_port = 1234

        self.pg_replication_slot = pg.PgReplicationSlot(
            self.source_host,
            self.source_port,
            self.replication_slot_name,
        )
        self.apply_patches([
            patch('gppylib.commands.pg.logger', return_value=mock_logger),
            patch('gppylib.db.dbconn.DbURL', return_value=Mock())
        ])

        self.mock_logger = self.get_mock_from_apply_patch('logger')

    @patch('gppylib.db.dbconn.connect', side_effect=Exception())
    def test_slot_exist_conn_exception(self, mock1):

        with self.assertRaises(Exception) as ex:
            self.pg_replication_slot.slot_exists()

        self.assertEqual(1, self.mock_logger.debug.call_count)
        self.assertEqual([call('Checking if slot internal_wal_replication_slot exists for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)
        self.assertTrue('Failed to query pg_replication_slots for' in str(ex.exception))

    @patch('gppylib.db.dbconn.connect', autospec=True)
    @patch('gppylib.db.dbconn.querySingleton', return_value=1)
    def test_slot_exist_query_true(self, mock1, mock2):
        self.assertTrue(self.pg_replication_slot.slot_exists())
        self.assertEqual(1, self.mock_logger.debug.call_count)
        self.assertEqual([call('Checking if slot internal_wal_replication_slot exists for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)

    @patch('gppylib.db.dbconn.connect', autospec=True)
    @patch('gppylib.db.dbconn.querySingleton', return_value=0)
    def test_slot_exist_query_false(self, mock1, mock2):
        self.assertFalse(self.pg_replication_slot.slot_exists())
        self.assertEqual(2, self.mock_logger.debug.call_count)
        self.assertEqual([call('Checking if slot internal_wal_replication_slot exists for host:bar, port:1234'),
                          call('Slot internal_wal_replication_slot does not exist for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)

    @patch('gppylib.db.dbconn.connect', side_effect=Exception())
    def test_drop_slot_conn_exception(self, mock1):
        with self.assertRaises(Exception) as ex:
            self.pg_replication_slot.drop_slot()

        self.assertEqual(1, self.mock_logger.debug.call_count)
        self.assertEqual([call('Dropping slot internal_wal_replication_slot for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)
        self.assertTrue('Failed to drop replication slot for host:bar, port:1234' in str(ex.exception))

    @patch('gppylib.db.dbconn.connect', autospec=True)
    @patch('gppylib.db.dbconn.query', side_effect=DatabaseError("DatabaseError Exception"))
    def test_drop_slot_db_error_exception(self, mock1, mock2):
        self.pg_replication_slot.drop_slot()
        self.assertEqual(1, self.mock_logger.debug.call_count)
        self.assertEqual(1, self.mock_logger.exception.call_count)
        self.assertEqual([call('Dropping slot internal_wal_replication_slot for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)
        self.assertEqual([call('Failed to query pg_drop_replication_slot for host:bar, port:1234: DatabaseError Exception')],
                         self.mock_logger.exception.call_args_list)

    @patch('gppylib.db.dbconn.connect', autospec=True)
    @patch('gppylib.db.dbconn.query', autospec=True)
    def test_drop_slot_success(self, mock1, mock2):
        self.assertTrue(self.pg_replication_slot.drop_slot())
        self.assertEqual(2, self.mock_logger.debug.call_count)
        self.assertEqual([call('Dropping slot internal_wal_replication_slot for host:bar, port:1234'),
                          call('Successfully dropped replication slot internal_wal_replication_slot for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)

    @patch('gppylib.db.dbconn.connect', side_effect=Exception())
    def test_create_slot_conn_exception(self, mock1):
        with self.assertRaises(Exception) as ex:
            self.pg_replication_slot.create_slot()

        self.assertEqual(1, self.mock_logger.debug.call_count)
        self.assertEqual([call('Creating slot internal_wal_replication_slot for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)
        self.assertTrue('Failed to create replication slot for host:bar, port:1234' in str(ex.exception))

    @patch('gppylib.db.dbconn.connect', autospec=True)
    @patch('gppylib.db.dbconn.query', side_effect=DatabaseError("DatabaseError Exception"))
    def test_create_slot_db_error_exception(self, mock1, mock2):
        self.pg_replication_slot.create_slot()
        self.assertEqual(1, self.mock_logger.debug.call_count)
        self.assertEqual(1, self.mock_logger.exception.call_count)
        self.assertEqual([call('Creating slot internal_wal_replication_slot for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)
        self.assertEqual(
            [call('Failed to query pg_create_physical_replication_slot for host:bar, port:1234: DatabaseError Exception')],
            self.mock_logger.exception.call_args_list)

    @patch('gppylib.db.dbconn.connect', autospec=True)
    @patch('gppylib.db.dbconn.query', autospec=True)
    def test_create_slot_success(self, mock1, mock2):
        self.assertTrue(self.pg_replication_slot.create_slot())
        self.assertEqual(2, self.mock_logger.debug.call_count)
        self.assertEqual([call('Creating slot internal_wal_replication_slot for host:bar, port:1234'),
                          call(
                              'Successfully created replication slot internal_wal_replication_slot for host:bar, port:1234')],
                         self.mock_logger.debug.call_args_list)

class TestUnitPgBaseBackup(unittest.TestCase):
    def test_replication_slot_not_passed_when_not_given_slot_name(self):
        base_backup = pg.PgBaseBackup(
            replication_slot_name = None,
            target_datadir="foo",
            source_host = "bar",
            source_port="baz",
            )

        tokens = base_backup.command_tokens

        self.assertNotIn("--slot", tokens)
        self.assertNotIn("some-replication-slot-name", tokens)
        self.assertIn("--wal-method", tokens)
        self.assertIn("fetch", tokens)
        self.assertNotIn("stream", tokens)

    @patch('gppylib.commands.pg.PgReplicationSlot.slot_exists', return_value=True)
    @patch('gppylib.commands.pg.PgReplicationSlot.drop_slot', return_value=True)
    def test_base_backup_passes_parameters_necessary_to_create_replication_slot_when_given_slotname(self, mock1, mock2):
        base_backup = pg.PgBaseBackup(
            create_slot=True,
            replication_slot_name='some-replication-slot-name',
            target_datadir="foo",
            source_host="bar",
            source_port="baz",
            )

        self.assertIn("--slot", base_backup.command_tokens)
        self.assertIn("some-replication-slot-name", base_backup.command_tokens)
        self.assertIn("--wal-method", base_backup.command_tokens)
        self.assertIn("stream", base_backup.command_tokens)
        self.assertIn("--create-slot", base_backup.command_tokens)

    @patch('gppylib.commands.pg.PgReplicationSlot.slot_exists', return_value=True)
    @patch('gppylib.commands.pg.PgReplicationSlot.drop_slot', return_value=False)
    def test_base_backup_passes_parameters_necessary_when_drop_replication_slot_ret_false(self, mock1, mock2):
        base_backup = pg.PgBaseBackup(
            create_slot=True,
            replication_slot_name='some-replication-slot-name',
            target_datadir="foo",
            source_host="bar",
            source_port="baz",
            )

        self.assertIn("--slot", base_backup.command_tokens)
        self.assertIn("some-replication-slot-name", base_backup.command_tokens)
        self.assertIn("--wal-method", base_backup.command_tokens)
        self.assertIn("stream", base_backup.command_tokens)
        self.assertNotIn("--create-slot", base_backup.command_tokens)

    @patch('gppylib.commands.pg.PgReplicationSlot.slot_exists', return_value=True)
    @patch('gppylib.commands.pg.PgReplicationSlot.drop_slot', return_value=True)
    def test_base_backup_does_not_pass_conflicting_xlog_method_argument_when_given_replication_slot(self, mock1, mock2):
        base_backup = pg.PgBaseBackup(
            create_slot=True,
            replication_slot_name='some-replication-slot-name',
            target_datadir="foo",
            source_host="bar",
            source_port="baz",
            )
        self.assertNotIn("-x", base_backup.command_tokens)
        self.assertNotIn("--xlog", base_backup.command_tokens)

class PgTests(GpTestCase):
    def setUp(self):
        self.apply_patches([
            patch('gppylib.commands.pg.logger', return_value=Mock(spec=['log', 'info', 'debug', 'error', 'warning'])),
        ])
        self.logger = self.get_mock_from_apply_patch('logger')

    @patch('gppylib.commands.pg.logger')
    @patch('gppylib.commands.pg.Command')
    @patch('gppylib.commands.pg.WorkerPool')
    def test_kill_existing_walsenders(self, mock_worker_pool, mock_command, mock_logger):
        primary_config = [("sdw1", 20000), ("sdw1", 20001)]
        batch_size = 5

        # Prepare mock return values
        mock_cmd_instance = mock_command.return_value
        mock_cmd_instance.get_results.return_value = None
        mock_cmd_instance.was_successful.return_value = True
        worker_pool_mock = MagicMock()
        worker_pool_mock.join.return_value = None
        mock_worker_pool.return_value = worker_pool_mock
        worker_pool_mock.getCompletedItems.return_value = [mock_cmd_instance]

        # Run the function being tested
        pg.kill_existing_walsenders_on_primary(primary_config, batch_size)

        # Assertions
        expected_calls = [
            call.info('killing existing walsender process on primary sdw1:20000'),
            call.info('killing existing walsender process on primary sdw1:20001')
        ]
        mock_logger.info.assert_has_calls(expected_calls)

        mock_worker_pool.assert_called_with(min(batch_size, 2))
        mock_cmd_instance.get_results.assert_called_once()
        mock_cmd_instance.was_successful.assert_called_once()
        mock_logger.warning.assert_not_called()


    @patch('gppylib.commands.pg.logger')
    @patch('gppylib.commands.pg.Command')
    @patch('gppylib.commands.pg.WorkerPool')
    def test_kill_existing_walsenders_failure(self, mock_worker_pool, mock_command, mock_logger):
        primary_config = [("sdw1", 20000), ("sdw1", 20001)]
        batch_size = 5

        # Prepare mock return values
        mock_cmd_instance = mock_command.return_value
        mock_cmd_instance.get_results.return_value = CommandResult(1, ''.encode(), 'error'.encode(), False, False)
        mock_cmd_instance.was_successful.return_value = False
        worker_pool_mock = MagicMock()
        worker_pool_mock.join.return_value = None
        mock_worker_pool.return_value = worker_pool_mock
        worker_pool_mock.getCompletedItems.return_value = [mock_cmd_instance]

        # Run the function being tested
        pg.kill_existing_walsenders_on_primary(primary_config, batch_size)

        # Assertions
        mock_worker_pool.assert_called_with(min(batch_size, 2))
        mock_cmd_instance.get_results.assert_called_once()
        mock_cmd_instance.was_successful.assert_called_once()
        mock_logger.warning.assert_called_once()
        args, _ = mock_logger.warning.call_args
        assert "Unable to kill walsender on primary" in args[0]


if __name__ == '__main__':
    run_tests()
