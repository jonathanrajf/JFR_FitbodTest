import csv
import pandas

"""
users.csv - This file contains the list of users

alias.csv - This file contains a mapping between users and their aliases. Aliases may be N levels deep
For e.g. User A -> Alias B -> Alias C

events.csv - This file contains feature assignments for users or their aliases.
There is a single feature key: feature-1 that has 2 variations: control & variation-1

Given a set of input user data, the goal is to join aliased users and find out what cohorts the users belong to.
We need to join aliased users to determine which variation a user is assigned to.
The final output should be a summary file containing the following:
 - user_count 
 - feature_key
 - feature_variation

"""
users_dict = {}
alias_reverse_dict = {}
with open("users.csv") as users_file:
    csv_reader = csv.reader(users_file, delimiter=',')
    next(csv_reader)  # Skip header

    for row in csv_reader:
        user_id = row[0]
        users_dict[user_id] = ''

# Build a reverse alias dict
with open("alias.csv") as alias_file:
    csv_reader = csv.reader(alias_file, delimiter=',')
    next(csv_reader)  # Skip header

    for row in csv_reader:
        # insert all aliases as keys
        alias = row[2]
        user_id = row[1]
        alias_reverse_dict[alias] = user_id

true_alias_dict = {}

for alias in alias_reverse_dict:
    mapped_user = alias_reverse_dict[alias]
    # This mapped user_id can either be a user_id or an alias_id or does not have a mapping
    if mapped_user in users_dict:
        # This is a final mapping. Add the alias to user map to the final dict
        true_alias_dict[alias] = mapped_user
    if mapped_user not in users_dict and mapped_user not in alias_reverse_dict:
        # A mapping does not exist for this alias. Save as mapped to 'UNKNOWN' to the final dict
        true_alias_dict[alias] = 'UNKNOWN_USER'
    else:
        # Keep looking for a user_id till user_id is present in the user_table
        cur_user_id = mapped_user
        while cur_user_id not in users_dict:
            cur_user_id = alias_reverse_dict[cur_user_id]

            if mapped_user not in users_dict and mapped_user not in alias_reverse_dict:
                # A mapping does not exist for this alias. Save as mapped to 'UNKNOWN' to the final dict
                true_alias_dict[alias] = 'UNKNOWN_USER'
                break

        true_alias_dict[alias] = cur_user_id

#Test to see if every value is present in user_id
# true_count = 0
# total_count = 0
# for key in true_alias_dict:
#     total_count +=1
#     if true_alias_dict[key] in users_dict:
#         true_count+=1
#
# print(f"Total count in {total_count} and true count is {true_count}")

# At this point every alias in true_alias_dict has a mapping to a user_id
event_list = []
with open("events.csv") as events_file:
    csv_reader = csv.reader(events_file, delimiter=',')
    next(csv_reader) # Skip header

    for row in csv_reader:
        user_id = row[1]
        if user_id in users_dict:
            event_list.append([user_id, row[2], row[3]])
        elif user_id in true_alias_dict:
            event_list.append([true_alias_dict[user_id], row[2], row[3]])
        else:
            event_list.append(['UNKNOWN_USER', row[2], row[3]])

# Convert to Pandas Dataframe
events_df = pandas.DataFrame(event_list, columns=['user_id', 'feature_key', 'feature_variation'])

grouped_events_df = pandas.DataFrame()

grouped_events_df['user_count'] = events_df.groupby(['feature_key', 'feature_variation'])['user_id'].nunique()

grouped_events_df.to_csv('output_counts.csv')
