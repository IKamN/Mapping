print(len('test_CRSForm_CB_CCP_PersonInfo.PrevName.MiddleName.Other'))

start = ['test_CRSForm', 'test_CRSForm_ClientCrs', 'test_CRSForm_CrsBeneficiaries', 'test_CRSForm_CrsControllingPersons', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons', 'test_CRSForm_CrsBeneficiaries_CrsPersonInfo.ClientCrs', 'test_CRSForm_CrsControllingPersons_CrsPersonInfo.ClientCrs', 'test_CRSForm_CrsBeneficiaries_PersonInfo.Name.FullName.Other', 'test_CRSForm_CrsBeneficiaries_PersonInfo.Name.LastName.Other', 'test_CRSForm_CrsBeneficiaries_PersonInfo.Name.FirstName.Other', 'test_CRSForm_CrsBeneficiaries_PersonInfo.Name.MiddleName.Other', 'test_CRSForm_CrsBeneficiaries_PersonInfo.PrevName.FullName.Other', 'test_CRSForm_CrsBeneficiaries_PersonInfo.PrevName.LastName.Other', 'test_CRSForm_CrsBeneficiaries_PersonInfo.PrevName.FirstName.Other', 'test_CRSForm_CrsControllingPersons_PersonInfo.Name.FullName.Other', 'test_CRSForm_CrsControllingPersons_PersonInfo.Name.LastName.Other', 'test_CRSForm_CrsBeneficiaries_PersonInfo.PrevName.MiddleName.Other', 'test_CRSForm_CrsControllingPersons_PersonInfo.Name.FirstName.Other', 'test_CRSForm_CrsControllingPersons_PersonInfo.Name.MiddleName.Other', 'test_CRSForm_CrsControllingPersons_PersonInfo.PrevName.FullName.Other', 'test_CRSForm_CrsControllingPersons_PersonInfo.PrevName.LastName.Other', 'test_CRSForm_CrsControllingPersons_PersonInfo.PrevName.FirstName.Other', 'test_CRSForm_CrsControllingPersons_PersonInfo.PrevName.MiddleName.Other', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_CrsPersonInfo.ClientCrs', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_PersonInfo.Name.FullName.Other', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_PersonInfo.Name.LastName.Other', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_PersonInfo.Name.FirstName.Other', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_PersonInfo.Name.MiddleName.Other', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_PersonInfo.PrevName.FullName.Other', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_PersonInfo.PrevName.LastName.Other', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_PersonInfo.PrevName.FirstName.Other', 'test_CRSForm_CrsBeneficiaries_CrsControllingPersons_PersonInfo.PrevName.MiddleName.Other']

def cut_string(start):
    if len(start) > 60:
        split_string = start.split('_')
        for j in range(2, len(split_string)):
            if any(c.isupper() for c in split_string[j]):
                new_substring = ''.join([s for s in split_string[j] if s.isupper()])
                split_string[j] = new_substring
            new_string = '_'.join(split_string)
            if len(new_string) <= 60:
                return new_string


for i in start:
    print(cut_string(i))