import pandas as pd

def process_cau_files(file_paths):
	'''
	Processing the cau files: nrf_file and headcount_file
	:param file_paths:
	:return:
		The merged data
	'''

	def find_ai_opt_in(ai_opt_group):
		'''
		Find an individuals who has opted in
		:param ai_opt_group:
		:return:
			An individual who has opted in
		'''
		# Convert effective date into datetime
		ai_opt_group['TIME'] = pd.to_datetime(ai_opt_group['Effective Date'] + ' ' + ai_opt_group['Modified Time'],
											  format='%d-%m-%Y %H:%M:%S')
		row_max = ai_opt_group[ai_opt_group['TIME'] == ai_opt_group['TIME'].max()]
		if len(row_max) == 1 and row_max['Wealth Indicator Value Code'].all() == 'Yes':
			return row_max
		return None

	validate_input_files(file_paths)

	try:
		result = {}
		# Get data from UTS file
		# For UTS, if “EBBS Rel ID 1” column does NOT start with “01” or “04”, then it is a non-individual transaction.
		# Following by: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.startswith.html
		uts_df = \
		map(lambda df: df[df.get('EBBS Rel ID 1').str.startswith("01") | df.get('EBBS Rel ID 1').str.startswith("04")],
			[pd.read_excel(file_paths['uts_file.xlsx'], sheet_name='DailyTransactionContactConfirme')])[0]

		# Get data from Banca file and remove all transactions are non-individual
		# For all Banca sheets, if “client name” column contains “pte” or “ltd”, then it is a non-individual transaction
		banca_sheets = [
			map(lambda df: df[
				~df.get('Owner Surname').str.contains("pte|ltd") & ~df.get('Owner Given Name 1').str.contains(
					"pte|ltd") & \
				~df.get('Owner Given Name 2').str.contains("pte|ltd") & ~df.get('Owner Christian Name').str.contains(
					"pte|ltd")
				], [pd.read_excel(file_paths['banca_file.xlsx'], sheet_name='Pru_FSC')])[0],
			map(lambda df: df[
				~df.get('Owner Surname').str.contains("pte|ltd") & ~df.get('Owner Given Name 1').str.contains(
					"pte|ltd") & \
				~df.get('Owner Given Name 2').str.contains("pte|ltd") & ~df.get('Owner Christian Name').str.contains(
					"pte|ltd")
				], [pd.read_excel(file_paths['banca_file.xlsx'], sheet_name='Pru_IS')])[0],
			map(lambda df: df[
				~df.get('Proposed Name').str.contains("pte|ltd")
			], [pd.read_excel(file_paths['banca_file.xlsx'], sheet_name='AIA')])[0],
			map(lambda df: df[
				~df.get('OWNER').str.contains("pte|ltd")
			], [pd.read_excel(file_paths['banca_file.xlsx'], sheet_name='Manulife')])[0],
			map(lambda df: df[
				~df.get('LIFE_ASSURED_NAME').str.contains("pte|ltd")
			], [pd.read_excel(file_paths['banca_file.xlsx'], sheet_name='HSBC')])[0],
		]

		# Get data from FINIQ file
		# For Finiq sheet: if “Sign Auth” column is NOT “SINGLE” or “EITHER/OR”, then it is a non-individual transaction.
		finiq_df = map(lambda df: df[df.get('Sign Auth').str.contains("SINGLE|EITHER/OR")],
					   [pd.read_excel(file_paths['finiq_file.xlsx'], sheet_name='Sheet1')])[0]

		# Get data from Structured Deposits file
		# For Structured Deposits, if “REL ID (PRIMARY)” column does NOT start with “01” or “04”, then it is a non-individual transaction.
		structured_deposit_df = map(lambda df: df[
			df.get('REL ID (PRIMARY)').str.startswith("01") | df.get('REL ID (PRIMARY)').str.startswith("04")], [
										pd.read_excel(file_paths['structured_deposits_file.xlsx'],
													  sheet_name='BOOKBUILDING')])[0]

		# Get data from AI file
		ai_df = [
			pd.read_excel(file_paths['ai_file.xlsx'], sheet_name='AI Eligibility'),
			pd.read_excel(file_paths['ai_file.xlsx'], sheet_name='ACOI')
		]
		# Merging data
		result['Representative'] = uts_df['Sales Staff Name'].to_list() + banca_sheets[0]['FSC'].to_list() + \
								   banca_sheets[1]['FSC'].to_list() + banca_sheets[2]['IS Name'].to_list() + \
								   banca_sheets[3]['AGENT NAME'].to_list() + banca_sheets[4]['AGENT'].to_list() + \
								   finiq_df['RM Dealing'].to_list() + structured_deposit_df['SALES PERSON'].to_list()

		result['Representative ID'] = uts_df['Order Closing Id'].to_list() + banca_sheets[0]['Bank ID'].to_list() + \
									  banca_sheets[1]['Bank ID'].to_list() + banca_sheets[2]['Bank ID'].to_list() + \
									  banca_sheets[3]['PeopleWise'].to_list() + banca_sheets[4][
										  'PeopleWise ID'].to_list() + finiq_df['RM ID'].to_list() + \
									  structured_deposit_df['peoplewise ID RM'].to_list()

		result['Client'] = \
			['{}-{}-{}-{}'.format(account1, account2, account3, account4) for account1, account2, account3, account4 in
			 zip(uts_df['Account Holder 1'].to_list(), uts_df['Account Holder 2'].to_list(),
				 uts_df['Account Holder 3'].to_list(), uts_df['Account Holder 4'].to_list())] + \
			['{}-{}-{}-{}'.format(owner_suname, owner_given_name_1, owner_given_name_2, owner_christian_name) for
			 owner_suname, owner_given_name_1, owner_given_name_2, owner_christian_name in
			 zip(banca_sheets[0]['Owner Surname'].to_list(), banca_sheets[0]['Owner Given Name 1'].to_list(),
				 banca_sheets[0]['Owner Given Name 2'].to_list(), banca_sheets[0]['Owner Christian Name'].to_list())] + \
			['{}-{}-{}-{}'.format(owner_suname, owner_given_name_1, owner_given_name_2, owner_christian_name) for
			 owner_suname, owner_given_name_1, owner_given_name_2, owner_christian_name in
			 zip(banca_sheets[1]['Owner Surname'].to_list(), banca_sheets[1]['Owner Given Name 1'].to_list(),
				 banca_sheets[1]['Owner Given Name 2'].to_list(), banca_sheets[1]['Owner Christian Name'].to_list())] + \
			banca_sheets[2]['Proposed Name'].to_list() + banca_sheets[3]['OWNER'].to_list() + banca_sheets[4][
				'LIFE_ASSURED_NAME'].to_list() + \
			['{}-{}-{}-{}'.format(main_name, joint_name1, joint_name2, joint_name3) for
			 main_name, joint_name1, joint_name2, joint_name3 in
			 zip(finiq_df['Main Name'].to_list(), finiq_df['Joint Name1'].to_list(), finiq_df['Joint Name2'].to_list(),
				 finiq_df['Joint Name3'].to_list())] + \
			['' for i in range(len(structured_deposit_df))]

		result['Client ID'] = \
			['{}|{}|{}|{}'.format(id1, id2, id3, id4) for id1, id2, id3, id4 in
			 zip(uts_df['EBBS Rel ID 1'].to_list(), uts_df['EBBS Rel ID 2'].to_list(),
				 uts_df['EBBS Rel ID 3'].to_list(), uts_df['EBBS Rel ID 4'].to_list())] + \
			banca_sheets[0]['Owner ID Num'].to_list() + banca_sheets[1]['Owner ID Num'].to_list() + banca_sheets[2][
				'Proposed NRIC'].to_list() + banca_sheets[3]['OWNER_NRIC'].to_list() + banca_sheets[4][
				'LIFE_NRIC'].to_list() + \
			['{}|{}|{}|{}'.format(doc_no, joint1_doc_id, joint2_doc_id, joint3_doc_id) for
			 doc_no, joint1_doc_id, joint2_doc_id, joint3_doc_id in
			 zip(finiq_df['Document No'].to_list(), finiq_df['Joint1 Doc ID'].to_list(),
				 finiq_df['Joint2 Doc ID'].to_list(), finiq_df['Joint3 Doc ID'].to_list())] + \
			['{}|{}|{}|{}'.format(id_pri, id1, id2, id3) for id_pri, id1, id2, id3 in
			 zip(structured_deposit_df['REL ID (PRIMARY)'].to_list(),
				 structured_deposit_df['REL ID (JOINT 1)'].to_list(),
				 structured_deposit_df['REL ID (JOINT 2)'].to_list(),
				 structured_deposit_df['REL ID (JOINT 3)'].to_list())]

		result['Product issuer'] = \
			['{} {}'.format(currency, amount) for currency, amount in
			 zip(uts_df['Asset Currency'].to_list(), uts_df['Transaction Amount'].to_list())] + \
			['{} {}'.format(currency, api if str(api).isdigit() else spi) for currency, api, spi, in
			 zip(banca_sheets[0]['Contract Plan Description'].to_list(), banca_sheets[0]['API'].to_list(),
				 banca_sheets[0]['SPI'].to_list())] + \
			['{} {}'.format(currency, api if str(api).isdigit() else spi) for currency, api, spi, in
			 zip(banca_sheets[1]['Contract Plan Description'].to_list(), banca_sheets[1]['API'].to_list(),
				 banca_sheets[1]['SPI'].to_list())] + \
			['{} {}'.format(currency, api if str(api).isdigit() else spi) for currency, api, spi, in
			 zip(banca_sheets[2]['Currency'].to_list(), banca_sheets[2]['API US$'].to_list(),
				 banca_sheets[2]['SPI US$'].to_list())] + \
			['{} {}'.format(currency, amount) for currency, amount, in
			 zip(banca_sheets[3]['CURRENCY'].to_list(), banca_sheets[3]['APE'].to_list())] + \
			['{} {}'.format(currency, amount) for currency, amount, in
			 zip(banca_sheets[4]['CURRENCY'].to_list(), banca_sheets[4]['PLANNED_PREMIUM'].to_list())] + \
			['{} {}'.format(currency, amount) for currency, amount, in
			 zip(finiq_df['Depo Ccy'].to_list(), finiq_df['Depo Amt'].to_list())] + \
			['{} {}'.format(currency, amount) for currency, amount, in
			 zip(structured_deposit_df['CCY'].to_list(), structured_deposit_df['AMOUNT'].to_list())]

		result['Product name'] = uts_df['Asset Name'].to_list() + banca_sheets[0][
			'Contract Plan Description'].to_list() + banca_sheets[1]['Contract Plan Description'].to_list() + \
								 banca_sheets[2]['Plan Name'].to_list() + banca_sheets[3]['POLICY NAME'].to_list() + [
									 'HSBC JADE' for i in range(len(banca_sheets[4]))] + \
								 ['{}-{}'.format(currency, amount) for currency, amount, in
								  zip(finiq_df['Depo Ccy'].to_list(), finiq_df['Alt Ccy'].to_list())] + \
								 structured_deposit_df['Product name'].to_list()

		result['Transaction number'] = uts_df['Order No.'].to_list() + banca_sheets[0]['Contract Number'].to_list() + \
									   banca_sheets[1]['Contract Number'].to_list() + banca_sheets[2][
										   'Contract Number (Policy No)'].to_list() + banca_sheets[3][
										   'POLICY NUMBER'].to_list() + banca_sheets[4]['CONTRACT_NUMBER'].to_list() + \
									   finiq_df['DealNo'].to_list() + ['{}-{}'.format(currency, amount) for
																	   currency, amount, in
																	   zip(structured_deposit_df['SEQ NO.'].to_list(),
																		   structured_deposit_df[
																			   'Branch code - Rotation no.'].to_list())]

		result['Issuance Date'] = uts_df['Order Date'].to_list() + banca_sheets[0]['Issuance Date'].to_list() + \
								  banca_sheets[1]['Issuance Date'].to_list() + banca_sheets[2][
									  'Inception Date'].to_list() + banca_sheets[3]['ISSUEDATE'].to_list() + \
								  banca_sheets[4]['ISSUEDATE'].to_list() + \
								  finiq_df['Transaction Date'].to_list() + structured_deposit_df[
									  'CM ORDER DATE'].to_list()

		result['Product category'] = uts_df['Account Type'].to_list() + ['Banca' for i in range(
			len(banca_sheets[0]) + len(banca_sheets[1]) + len(banca_sheets[2]) + len(banca_sheets[3]) + len(
				banca_sheets[4]))] + ['FINIQ' for i in range(len(finiq_df))] + ['SD' for i in
																				range(len(structured_deposit_df))]

		result['Excluded from Sampling base'] = result['Mandatory Sampling'] = result[
			'Quarter Gross Commission Entitled'] = result['Total Gross Commission Entitled'] = ['' for i in range(
			len(uts_df) + len(banca_sheets[0]) + len(banca_sheets[1]) + len(banca_sheets[2]) + len(
				banca_sheets[3]) + len(banca_sheets[4]) + len(finiq_df) + len(structured_deposit_df))]

		# Remove all transactions are AI transaction
		# Get data from AI file
		# See: Transactions by Accredited Investors (AI) in FSD doc
		# Only keep the row that has latest record by “Effective Date” and “Modified Time” in ACOI sheet
		# Following by: https://stackoverflow.com/questions/12497402/python-pandas-remove-duplicates-by-columns-a-keeping-the-row-with-the-highest
		ai_sheets = [
			pd.read_excel(file_paths['ai_file.xlsx'], sheet_name='AI Eligibility'),
			pd.read_excel(file_paths['ai_file.xlsx'], sheet_name='ACOI').groupby('Relationship No').apply(
				lambda x: find_ai_opt_in(x)).drop_duplicates()
		]

		ai_transaction_index = []
		if not ai_sheets[1].empty:

			for i in range(len(result['Client ID'])):  # For testing
				# Get list of client-ids and transaction date from the transaction
				client_ids = result['Client ID'][i].split('|')
				transaction_date = result['Issuance Date'][i]
				# If any of clients is NOT AI OPT-IN, The transaction is NOT an AI TRANSACTION
				all_opt_in = True
				has_opt_in = False
				for client_id in client_ids:
					# If any of clients is NOT AI OPT-IN then the transaction is NOT an AI TRANSACTION
					if client_id not in ai_sheets[1]['Relationship No'].to_list():
						all_opt_in = False
						break
					# Check if any of clients is AI ELIGIBLE then the transaction is an AI TRANSACTION
					# Get rows are AI ELIGIBLE
					ai_eligible_rows = ai_sheets[0].loc[
						(ai_sheets[0]['Relationship No'] == client_id) & \
						(pd.to_datetime(ai_sheets[0]['Effective Date'], format='%d-%m-%Y') <= pd.to_datetime(
							transaction_date, format='%d-%m-%Y')) & \
						(pd.to_datetime(transaction_date, format='%d-%m-%Y') <= pd.to_datetime(
							ai_sheets[0]['Expiry Date'], format='%d-%m-%Y'))
						]

					if len(ai_eligible_rows) > 0:
						has_opt_in = True

				if all_opt_in and has_opt_in:
					ai_transaction_index.append(i)

		# Remove all ai transactions in result data
		for i in ai_transaction_index:
			for key, value in result.iteritems():
				# All variables bellow use same memory with 'Ecluded from Sampling base' variable
				if key not in ['Mandatory Sampling', 'Quarter Gross Commission Entitled',
							   'Total Gross Commission Entitled']:
					del result[key][i]
	except:
		traceback.print_exc()

	return result
