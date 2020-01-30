using Microsoft.WindowsAzure.Storage.Table;

namespace ContactsCoreMVC.Models.Entities
{
    public class ContactTable : TableEntity
    {
        public ContactTable()
        {

        }
        public ContactTable(string contactName, string phone)
        {
            PartitionKey = contactName;
            RowKey = phone;
        }

        public string ContactType { get; set; }
        public string Email { get; set; }
    }
}
