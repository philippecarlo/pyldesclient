import sys
import traceback
import argparse
from argparse import Namespace
from ldes_client_error import LdesClientError
from dependency_injector.wiring import inject, Provide
from container import Container


def configure_arg_parser() -> Namespace:
    parser = argparse.ArgumentParser(description='Linked Data Event Stream (LDES) client library and CLI for python.')
    subparsers = parser.add_subparsers(dest='command')
    parser.add_argument('--verbose', action='count', help='Control client logging verbosity.')
    # start sync command
    parser_start = subparsers.add_parser('sync', help='Start synchronizing a previously onboarded LDES collection with given alias.')
    parser_start.add_argument('alias', help='The alias of the LDES view/collection.')
    # stop sync command
    parser_start = subparsers.add_parser('stop', help='Stop synchronizing a previously onboarded LDES collection with given alias.')
    parser_start.add_argument('alias', help='The alias of the LDES view/collection.')
    # status command
    parser_status = subparsers.add_parser('status', help='Displays the sync status of an LDES collection with given alias.')
    parser_status.add_argument('alias', help='The alias of the LDES view/collection.', nargs='*')
    # delete command
    parser_delete = subparsers.add_parser('delete', help='Delete the state of locally synced LDES collection with given alias.')
    parser_delete.add_argument('alias', help='The alias of the LDES collection that is to be deleted. WARNING: this clears the entire LDES state.')
    # onboard command
    parser_onboard = subparsers.add_parser('onboard', help='Onboard an LDES collection with given alias.')
    parser_onboard.add_argument('location', help='The URL of the LDES view/collection.')
    parser_onboard.add_argument('alias', help='The alias of the LDES view/collection.')
    parser_onboard.add_argument('--polling', default='60', type=int, help='The polling interval in seconds.')
    return parser

def onboard_ldes(
        alias: str,
        location: str,
        polling: int = 60
    ):
    ldes_manager = container.ldes_manager_factory(alias=alias, ldes_store__location=container.config.storage.location(),ldes_store__alias=alias)
    ldes_manager.onboard_ldes_view(alias, location, polling)

def sync_ldes(alias: str):
    ldes_syncer = container.ldes_syncer_factory(alias=alias, ldes_store__location=container.config.storage.location(), ldes_store__alias=alias)
    ldes_syncer.sync()

def stop_sync_ldes(alias: str):
    ldes_syncer = container.ldes_syncer_factory(alias=alias, ldes_store__location=container.config.storage.location(), ldes_store__alias=alias)
    ldes_syncer.stop_sync()

def delete_ldes(alias: str):
    ldes_manager = container.ldes_manager_factory(alias=alias, ldes_store__location=container.config.storage.location(),ldes_store__alias=alias)
    ldes_manager.delete_ldes_view()    

if __name__ == '__main__':
    parser = configure_arg_parser()
    cl_args = parser.parse_args()
    #print (cl_args)
    container = Container()
    try:
        if cl_args.command == 'sync':
            print (f'Starting LDES client for view or subset {cl_args.alias}.')
            sync_ldes(cl_args.alias)
        if cl_args.command == 'stop':
            print (f'Stop syncing view or subset {cl_args.alias}.')
            stop_sync_ldes(cl_args.alias)
        elif cl_args.command == 'onboard':
            print (f'Onboarding LDES view or subset {cl_args.location} as {cl_args.alias} with polling interval {cl_args.polling}.')
            onboard_ldes(cl_args.alias, cl_args.location, cl_args.polling)
        elif cl_args.command == 'delete':
            print (f'Deleting LDES view or subset with alias {cl_args.alias}.')
            delete_ldes(cl_args.alias)
        elif cl_args.command == 'status':
            print (f'Getting LDES status for view or subset {cl_args.alias} ... .')
    except LdesClientError as error:
        print(error, file=sys.stderr)
        traceback.print_exc()
