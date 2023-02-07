import json

from followthemoney import model
import pandas as pd


data = pd.read_excel('procurements_contracts_info_2022_12_08.xlsx')


def convert_row_customer(row):
    customer = model.make_entity('Company')
    customer.make_id(row.get('name'), row.get('customer_info'))

    customer.add('registrationNumber', row.get('customer_inn'))
    customer.add('kppCode', row.get('customer_kpp'))
    customer.add('phone', row.get('customer_phone_list'))
    customer.add('website', row.get('customer_url'))
    yield customer


def convert_row_supplier(row):
    supplier = model.make_entity('Company')
    supplier.make_id(row.get('name'), row.get('supplier_name'))

    supplier.add('jurisdiction','ru')
    supplier.add('registrationNumber', row.get('supplier_inn'))
    supplier.add('email', row.get('supplier_emails_list'))
    supplier.add('phone', row.get('supplier_phone_list'))
    supplier.add('website', row.get('supplier_url'))
    yield supplier


def convert_row_contract(row):
    contract = model.make_entity('Contract')
    contract.make_id(row.get('num'), row.get('contract_num'))

    contract.add('title', row.get('contract_info'))
    contract.add('contractDate', row.get('contract_sign_date'))
    contract.add('contractDate', row.get('contract_end_date'))
    yield contract


# creating chunks
def batch(iterable, batch_number=10):
    length = len(iterable)
    for idx in range(0, length, batch_number):
        if isinstance(iterable, pd.DataFrame):
            yield iterable.iloc[idx:min(idx + batch_number, length)]
        else:
            yield iterable[idx:min(idx + batch_number, length)]


# for batch processing
def companies_iterator(df):
    for chunk in batch(df, 100):
        for row in chunk.to_dict(orient='records'):
            yield from convert_row_customer(row)
            yield from convert_row_contract(row)
            yield from convert_row_supplier(row)


if __name__ == '__main__':
    lst = []
    g = companies_iterator(data)
    with open('output.json', 'w+') as f:
        for x in g:
            lst.append(x.to_dict())

        res = list({v['id']: v for v in lst}.values())
        f.write('[')
        for r in res:
            f.write(json.dumps(r))
            if r != res[-1]:
                f.write(',\n')
            else:
                f.write('\n')
        f.write(']')
