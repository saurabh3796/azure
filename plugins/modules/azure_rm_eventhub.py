#!/usr/bin/python
#
# Copyright (c) 2021 Saurabh Malpani (@saurabh3796)
#
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
from ansible_collections.azure.azcollection.plugins.module_utils.azure_rm_common import AzureRMModuleBase

__metaclass__ = type

try:
    from msrestazure.azure_exceptions import CloudError
    from azure.mgmt.eventhub.models import EventHubCreateOrUpdateParameters, NamespaceCreateOrUpdateParameters
    from azure.mgmt.eventhub.models.sku import Sku
except ImportError:
    # This is handled in azure_rm_common
    pass

capture_description_spec = dict(
    enabled=dict(type='bool', required=True),
    encoding=dict(choices=['AVRO', 'AVRO_DEFLATE'], type='str', required=True)
)

partition_count_list = [i for i in range(1, 33)]


class AzureNotificationHub(AzureRMModuleBase):

    def __init__(self):
        # define user inputs from playbook

        self.authorizations_spec = dict(
            name=dict(type='str', required=True)
        )

        self.module_arg_spec = dict(
            message_retention_in_days=dict(type='int'),
            capture_description=dict(
                required=False,
                options=capture_description_spec

            ),
            name=dict(type='str'),
            namespace_name=dict(type='str', required=True),
            partition_count=dict(type='int', choices=partition_count_list),
            resource_group=dict(type='str', required=True),
            sku=dict(type='str', choices=[
                     'free', 'basic', 'standard'], default='free'),
            status=dict(choices=["Active", "Disabled", "Restoring", "SendDisabled", "ReceiveDisabled", "Creating", "Deleting", "Renaming", "Unknown"],
                        default='Active', type='str'),
            state=dict(choices=['present', 'absent'],
                       default='present', type='str'),
            location=dict(type='str')
        )
        required_if = [
            ('state', 'present', [
             'partition_count', 'message_retention_in_days'])
        ]
        self.sku = None
        self.resource_group = None
        self.namespace_name = None
        self.message_retention_in_days = None
        self.name = None
        self.location = None
        self.authorizations = None
        self.tags = None
        self.status = None
        self.partition_count = None
        self.results = dict(
            changed=False,
            state=dict()
        )
        self.state = None

        super(AzureNotificationHub, self).__init__(derived_arg_spec=self.module_arg_spec,
                                                   supports_check_mode=True,
                                                   supports_tags=True)

    def exec_module(self, **kwargs):
        print(":::::::::::::::::::::::")
        for key in list(self.module_arg_spec.keys()) + ['tags']:
            setattr(self, key, kwargs[key])

        # retrieve resource group to make sure it exists
        self.get_resource_group(self.resource_group)

        results = dict()
        changed = False

        try:
            self.log(
                'Fetching Event Hub Namespace {0}'.format(self.name))
            namespace = self.event_hub_client.namespaces.get(
                self.resource_group, self.namespace_name)

            results = namespace_to_dict(namespace)
            if self.name:
                self.log('Fetching event Hub {0}'.format(self.name))
                notification_hub = self.event_hub_client.event_hubs.get(
                    self.resource_group, self.namespace_name, self.name)
                notification_hub_results = notification_hub_to_dict(
                    notification_hub)
            # don't change anything if creating an existing namespace, but change if deleting it
            print("*************************")
            print(notification_hub)
            if self.state == 'present':
                changed = False

                update_tags, results['tags'] = self.update_tags(
                    results['tags'])

                if update_tags:
                    changed = True
                elif self.namespace_name and not self.name:
                    if self.sku != results['sku'].lower():
                        changed = True

            elif self.state == 'absent':
                changed = True

        except CloudError:
            # the notification hub does not exist so create it
            if self.state == 'present':
                changed = True
            else:
                # you can't delete what is not there
                changed = False

        self.results['changed'] = changed
        if self.name and not changed:
            self.results['state'] = notification_hub_results
        else:
            self.results['state'] = results

        # return the results if your only gathering information
        if self.check_mode:
            return self.results

        if changed:
            if self.state == "present":
                if self.name is None:
                    self.results['state'] = self.create_or_update_namespaces()
                elif self.namespace_name and self.name:
                    self.results['state'] = self.create_or_update_notification_hub()
            elif self.state == "absent":
                # delete Notification Hub
                if self.name is None:
                    self.delete_namespace()
                elif self.namespace_name and self.name:
                    self.delete_notification_hub()
                self.results['state']['status'] = 'Deleted'

        return self.results

    def create_or_update_namespaces(self):
        '''
        create or update namespaces
        '''
        try:
            namespace_params = NamespaceCreateOrUpdateParameters(
                location=self.location,
                namespace_type="EventHub",
                sku=Sku(name=self.sku),
                tags=self.tags
            )
            result = self.event_hub_client.namespaces.create_or_update(
                self.resource_group,
                self.namespace_name,
                namespace_params)

            namespace = self.event_hub_client.namespaces.get(
                self.resource_group,
                self.namespace_name)

            while namespace.status == "Created":
                time.sleep(30)
                namespace = self.event_hub_client.namespaces.get(
                    self.resource_group,
                    self.namespace_name,
                )
        except CloudError as ex:
            self.fail("Failed to create namespace {0} in resource group {1}: {2}".format(
                self.namespace_name, self.resource_group, str(ex)))
        return namespace_to_dict(result)

    def create_or_update_notification_hub(self):
        '''
        Create or update Notification Hub.
        :return: create or update Notification Hub instance state dictionary
        '''
        try:
            response = self.create_or_update_namespaces()
            params = EventHubCreateOrUpdateParameters(
                location=self.location,
                name=self.name,
                message_retention_in_days=self.message_retention_in_days,
                partition_count=self.partition_count,
                status=self.status
            )
            result = self.event_hub_client.notification_hubs.create_or_update(
                self.resource_group,
                self.namespace_name,
                self.name,
                params)
            self.log("Response : {0}".format(result))
        except CloudError as ex:
            self.fail("Failed to create notification hub {0} in resource group {1}: {2}".format(
                self.name, self.resource_group, str(ex)))
        return notification_hub_to_dict(result)

    def delete_notification_hub(self):
        '''
        Deletes specified notication hub
        :return True
        '''
        self.log("Deleting the notification hub {0}".format(self.name))
        try:
            print("inside delete event hub")
            poller = self.event_hub_client.event_hubs.delete(
                self.resource_group, self.namespace_name, self.name)
            result = self.get_poller_result(poller)
        except CloudError as e:
            self.log('Error attempting to delete notification hub.')
            self.fail(
                "Error deleting the notification hub : {0}".format(str(e)))
        return True

    def delete_namespace(self):
        '''
        Deletes specified namespace
        :return True
        '''
        self.log("Deleting the namespace {0}".format(self.namespace_name))
        try:
            poller = self.event_hub_client.namespaces.begin_delete(
                self.resource_group, self.namespace_name)
            result = self.get_poller_result(poller)
        except CloudError as e:
            self.log('Error attempting to delete namespace.')
            self.fail(
                "Error deleting the namespace : {0}".format(str(e)))
        return True


def notification_hub_to_dict(item):
    # turn event hub object into a dictionary (serialization)
    event_hub = item.as_dict()
    result = dict()
    if item.additional_properties:
        result['additional_properties'] = item.additional_properties
    result['name'] = event_hub.get('name', None)
    result['partition_ids'] = event_hub.get('partition_ids', None)
    result['created_at'] = event_hub.get('created_at', None)
    result['updated_at'] = event_hub.get('updated_at', None)
    result['message_retention_in_days'] = event_hub.get(
        'message_retention_in_days', None)
    result['partition_count'] = event_hub.get('partition_count', None)
    result['status'] = event_hub.get('status', None)
    result['tags'] = event_hub.get('tags', None)
    return result


def namespace_to_dict(item):
    # turn notification hub namespace object into a dictionary (serialization)
    namespace = item.as_dict()
    result = dict(
        additional_properties=namespace.get(
            'additional_properties', {}),
        name=namespace.get('name', None),
        type=namespace.get('type', None),
        location=namespace.get(
            'location', '').replace(' ', '').lower(),
        sku=namespace.get("sku").get("name"),
        tags=namespace.get('tags', None),
        provisioning_state=namespace.get(
            'provisioning_state', None),
        region=namespace.get('region', None),
        metric_id=namespace.get('metric_id', None),
        service_bus_endpoint=namespace.get(
            'service_bus_endpoint', None),
        scale_unit=namespace.get('scale_unit', None),
        enabled=namespace.get('enabled', None),
        critical=namespace.get('critical', None),
        data_center=namespace.get('data_center', None),
        namespace_type=namespace.get('namespace_type', None),
        updated_at=namespace.get('updated_at', None),
        created_at=namespace.get('created_at', None),
        is_auto_inflate_enabled=namespace.get(
            'is_auto_inflate_enabled', None),
        maximum_throughput_units=namespace.get(
            'maximum_throughput_units', None)
    )
    return result


def main():
    AzureNotificationHub()


if __name__ == '__main__':
    main()
