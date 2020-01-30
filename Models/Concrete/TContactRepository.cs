using System.Threading.Tasks;
using System.Collections.Generic;
using ContactsCoreMVC.Models.Abstract;
using ContactsCoreMVC.Models.Entities;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace ContactsCoreMVC.Models.Concrete
{
    public class TContactRepository : IContactRepository
    {
        private readonly CloudStorageAccount _cloudStorageAccount;
        private readonly CloudTableClient _cloudTableClient;
        private readonly CloudTable _cloudTable;

        public TContactRepository(IOptions<StorageUtility> storageUtility)
        {
            _cloudStorageAccount = storageUtility.Value.StorageAccount;
            _cloudTableClient = _cloudStorageAccount.CreateCloudTableClient();
            _cloudTable = _cloudTableClient.GetTableReference("Contacts");
            _cloudTable.CreateIfNotExistsAsync().GetAwaiter().GetResult();
        }

        public async Task<ContactTable> CreateAsync(ContactTable contactTable)
        {
            TableOperation insertOperation = TableOperation.Insert(contactTable);
            TableResult tableResult = await _cloudTable.ExecuteAsync(insertOperation);
            ContactTable insertedContact = tableResult.Result as ContactTable;
            return insertedContact;
        }

        public async Task<List<ContactTable>> GetAllContactsAsync()
        {
            TableQuery<ContactTable> query = new TableQuery<ContactTable>();
            TableContinuationToken tocken = null;
            var result = await _cloudTable.ExecuteQuerySegmentedAsync(query, tocken);
            return result.Results;
        }

        public async Task<ContactTable> FindContactAsync(string partitionKey, string rowKey)
        {
            TableOperation findOperation = TableOperation.Retrieve<ContactTable>(partitionKey, rowKey);
            TableResult tableResult = await _cloudTable.ExecuteAsync(findOperation);
            var result = tableResult.Result as ContactTable;
            return result;
        }

        public async Task<List<ContactTable>> FindContactByRowKeyAsync(string rowKey)
        {
            TableQuery<ContactTable> query = new TableQuery<ContactTable>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));
            TableContinuationToken tocken = null;
            var result = await _cloudTable.ExecuteQuerySegmentedAsync(query, tocken);
            return result.Results;
        }

        public async Task<List<ContactTable>> FindContactsByPartitionKeyAsync(string partitionKey)
        {
            TableQuery<ContactTable> query = new TableQuery<ContactTable>()
                                                 .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            TableContinuationToken tocken = null;
            var result = await _cloudTable.ExecuteQuerySegmentedAsync(query, tocken);
            return result.Results;
        }

        public async Task<ContactTable> UpdateAsync(ContactTable contactTable)
        {
            TableOperation updateOperation = TableOperation.Retrieve<ContactTable>(contactTable.PartitionKey, contactTable.RowKey);
            TableResult tableResult = await _cloudTable.ExecuteAsync(updateOperation);
            ContactTable retrievedContact = tableResult.Result as ContactTable;
            if (retrievedContact != null)
            {
                retrievedContact.ContactType = contactTable.ContactType;
                retrievedContact.Email = contactTable.Email;
                TableOperation updatedContact = TableOperation.Replace(retrievedContact);
                var updateResult = await _cloudTable.ExecuteAsync(updatedContact);
            }

            return null;
        }

        public async Task DeleteAsync(string partitionKey, string rowKey)
        {
            TableOperation findOperation = TableOperation.Retrieve<ContactTable>(partitionKey, rowKey);
            TableResult tableResult = await _cloudTable.ExecuteAsync(findOperation);
            var contact = tableResult.Result as ContactTable;
            if (contact != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(contact);
                var result = await _cloudTable.ExecuteAsync(deleteOperation);
            }

        }
    }
}