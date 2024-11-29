from collections import defaultdict

import psycopg2
from psycopg2 import Error
import pandas as pd
from sqlalchemy import create_engine, Column
import matplotlib.pyplot as plt
import geopy.distance




try:
    connection = psycopg2.connect(
        database="group_15_2024",  # TO BE REPLACED
        user="group_15_2024",  # TO BE REPLACED
        password="87k2xil8cZQv",  # TO BE REPLACED
        host="dbcourse.cs.aalto.fi",
        port="5432",
    )
    connection.autocommit = True

    database="group_15_2024"  # TO BE REPLACED
    user="group_15_2024"  # TO BE REPLACED
    password="87k2xil8cZQv"  # TO BE REPLACED
    host="dbcourse.cs.aalto.fi"
    port="5432"

    DIALECT = 'postgresql+psycopg2://'
    db_uri = "%s:%s@%s/%s" % (user, password, host, database)
    engine = create_engine(DIALECT + db_uri)
    conn=engine.connect()

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    # Fetch result

    #Exercise A.1: The starting date and the end date is joined with the title using concat
    a1 = (""" select id, concat(r.title, ' - from ', r.start_date, ' to ', r.end_date) as result
            from request r """)
    cursor.execute(a1)

    #Exercise A.2: The exercise is done under the assumption that for each request each valid applicant's
    #id is displayed next to the number of skills said applicant has that match with the request. The
    # count(concat(rs.request_id, ' ', sa.volunteer_id)) is used to count how many skills the applicant has
    #that match with the request
    a2 = (""" select rs.request_id, sa.volunteer_id , count(concat(rs.request_id, ' ', sa.volunteer_id)) as num_of_skills
            from request_skill rs, skill_assignment sa, volunteer_application va  
            where sa.skill_name = rs.skill_name and rs.request_id = va.request_id and va.is_valid = true and va.volunteer_id = sa.volunteer_id
            group by rs.request_id, sa.volunteer_id 
            order by request_id asc, num_of_skills desc """)
    cursor.execute(a2)
    #print(pd.read_sql_query(a2,conn))


    #Exercise A.3: The query returns the request id, name of the skill requested, and the number of missing volunteers for that requested skill.
    #The number of missing number of volunteers, named missing_number, was obtained by first grouping by the request Id, requested skill's name, and requested skill's minimum need
    #and the subtracting the number of distinct volunteers who have applied to the request and possess the requested skill from the minumum need of that requested skill.
    a3 = ("""
          select r.id, rs.skill_name, (rs.min_need - count(distinct va.volunteer_id)) as missing_number  
          from request r, request_skill rs, volunteer_application va, skill_assignment sa 
          where (r.id = rs.request_id) and (r.id = va.request_id) and (va.volunteer_id = sa.volunteer_id) and (sa.skill_name  = rs.skill_name) 
          group by r.id, rs.skill_name, rs.min_need  
          having (rs.min_need - count(distinct va.volunteer_id)) > 0 
          order by r.id; 
        """)
    cursor.execute(a3)
    #print(pd.read_sql_query(a3,conn))


    #Exercise A.4
    #Assumption: I don't know what is meant by closest date so I assume that the closest date must have been after the database is established,
    #therefore after the data is available so that's why I odered it from the latest day down
    a4 = ("""select r.title, r.id, r.priority_value, r.register_by_date , r.beneficiary_id 
            from request r 
            order by r.priority_value desc, r.register_by_date desc""")
    cursor.execute(a4)

    #Exercise A.5: This query returns the volunteer ID and every request within the range with at least 2 matching skills or no skills required.
    #This tasks was accommplished through the union of 2 subqueries: 
    # the first one searches for requests within each volunteer's range that have at least 2 matching skills and
    # the second one finds requests within each volunteer's range that do not have require skills. 
    #For the first subquery, we joined the tables volunteer_range, skill_assignment, request_location, and request_skill, 
    # and counted the number of distinct skill names which were grouped by the volunteer and request Ids.
    #For the second subquery, we joined volunteer_range to request_location, and returned the request Ids that do not appear in the request_skill table
    # for each volunteer Id, as that would mean the requests do not require any skill.
    a5 = ("""
          select * 
          from (
            (select m.volunteer_id, m.request_id 
            from (
                select vr.volunteer_id, rl.request_id, count(distinct sa.skill_name) as matching_skills 
                from volunteer_range vr, skill_assignment sa, request_location rl, request_skill rs 
                where (vr.volunteer_id = sa.volunteer_id) and (vr.city_id = rl.city_id) and (rl.request_id = rs.request_id) and (sa.skill_name = rs.skill_name) 
                group by vr.volunteer_id, rl.request_id
            ) as m
            where m.matching_skills >= 2)
            union 
            (select vr.volunteer_id, rl.request_id
            from volunteer_range vr, request_location rl 
            where (vr.city_id = rl.city_id) and (rl.request_id not in (select rs.request_id from request_skill rs)))
          ) as c
          order by c.volunteer_id, c.request_id;
        """)
    cursor.execute(a5)
    #print(pd.read_sql_query(a5,conn))

    #Exercise A.6: This query returns volunteer Ids, the Ids of requests that are still available for registration and whose title matches the volunteer's interest and, and the request title.
    #To compare the interest names to the titles of requests, the two column names were modified: 
    # For the request titles, the word "need" and space between each word were removed with the substring and replace functions, respectively, before 
    # the upper function was used convert all lowercase letters to their uppercase counterparts. 
    # For the interest names, only the upper function was used to convert lowercase letters to uppercase. 
    #To ensure that the requests are still available for register, we used the current_date function to obtain the current date 
    # and compared it with the register-by dates.
    a6 = ("""
          select ia.volunteer_id, r.id, r.title 
          from interest_assignment ia, request r
          where (upper(ia.interest_name) = upper(replace(substring(r.title from 1 for length(r.title) - 6), ' ', ''))) and (current_date < r.register_by_date )
          order by ia.volunteer_id, r.title, r.id;
        """)
    cursor.execute(a6)
    #print(pd.read_sql_query(a6,conn))
    
    #Exercise A.7: The first 3 lines return the results when some of the applicant's range does not match
    #with the request location, SOME but not ALL. This is when the subquery comes into action, by using NOT IN,
    # it will filter out the ones that have the range which match with request from the original 3 query lines
    a7 = (""" select distinct  rl.request_id, va.volunteer_id , v."name", v.email , v.travel_readiness 
            from volunteer_application va, volunteer_range vr, request_location rl, volunteer v 
            where va.volunteer_id = vr.volunteer_id and vr.city_id != rl.city_id and va.request_id = rl.request_id and v.id = va.volunteer_id 
            and (rl.request_id, va.volunteer_id , v."name", v.email , v.travel_readiness) not in(
                select distinct  rl.request_id, va.volunteer_id , v."name", v.email , v.travel_readiness
                from volunteer_application va, volunteer_range vr, request_location rl, volunteer v 
                where va.volunteer_id = vr.volunteer_id and vr.city_id = rl.city_id and va.request_id = rl.request_id and v.id = va.volunteer_id)
            order by v.travel_readiness desc """)
    cursor.execute(a7)

    #Exercise A.8
    #Assumption:
    a8 = (""" select rs.skill_name, sum(rs.value) / count(rs.skill_name) as co 
            from request_skill rs 
            group by rs.skill_name 
            order by co desc """)
    cursor.execute(a8)

    #Exercise A.9-12 (Free choices)

    #A.9: The query retrieves the data of volunteers who do not have applications.
    #Justification: This helps identify inactive volunteers so that the FRC organization can encourage them to participate in volunteer work.
    a9 = (""" 
        select *
        from volunteer v 
        where v.id not in (select va.volunteer_id from volunteer_application va);
        """)
    cursor.execute(a9)
    
    #A.10: The query searches up the beneficiaries, the skills they have requested for, and the number of requests for each skill.
    #Justificaiton: This helps identify the skills that each beneficiary needs most frequently, enabling the organization to better tailor support towards them. 
    a10 = ("""
        select r.beneficiary_id, rs.skill_name, count(distinct r.id) as number_of_requests
        from request r, request_skill rs 
        where r.id = rs.request_id
        group by r.beneficiary_id, rs.skill_name 
        order by r.beneficiary_id asc, number_of_requests desc;
        """)
    cursor.execute(a10)

    #A.11: The query returns the number of application that each volunteer has.
    #Justification: This helps notify the organization which volunteers are the most active, which gives said volunteers recognization for their work 
    #and may encourage other volunteers to be more active. 
    a11 = ("""
        select va.volunteer_id, count(distinct va.id) as number_of_applications
        from volunteer_application va 
        group by va.volunteer_id 
        order by number_of_applications desc;
        """)
    cursor.execute(a11)

    #A.12: For each city the query return the request in those city from most prioritized to least. This
    #is to help the city authorities allocate resource and easier management of volunteering activities
    a12 = ("""select rl.city_id, r.id, r.priority_value 
            from request r, request_location rl 
            where r.id = rl.request_id 
            order by rl.city_id asc, priority_value desc """)
    cursor.execute(a12)

    #Exercise B.a.1: This view lists next to each beneficiary the averages of the following: 
    # number of volunteers applied, age of volunteers, and number of volunteers needed per request.
    #The number of applicants per request was obtained using the subquery 'na' where distinct application Ids were counted 
    # after being grouped by the request Id. This subquery was then joined to request, volunteer_application, 
    # and volunteer and averaging with the function avg after grouping by beneficiary Id.
    #The age of the volunteer was obtained with the age and date_part functions before being averaged with the function avg.
    #The average number of volunteers was obtained by using the function avg on r.number_of_volunteers which we assumed to be the average number of volunteers needed. 
    # We had also tried the method of creating a subquery where we grouped request_skill by request Id and summing the minimum need of each skill together
    # before we averaged it in the main query. However, upon reading the project description, we came to believe that averaging r.number_of_volunteers was the correct action.
    view1 = ("""
            create or replace view avg_per_beneficiary as 
             select r.beneficiary_id, avg(na.number_of_applications) as avg_number_of_applications, avg(date_part('year', age(v.birthdate))) as avg_age, avg(r.number_of_volunteers) as avg_number_needed
             from request r, volunteer_application va, volunteer v, 
                (select va.request_id , count(distinct va.id) as number_of_applications from volunteer_application va group by va.request_id) as na
             where (r.id = va.request_id) and (va.volunteer_id = v.id) and (va.request_id = na.request_id)
             group by r.beneficiary_id 
             order by r.beneficiary_id;
             """)
    cursor.execute(view1)

    #Reason: Taking inspiration from a2, the query return for each volunteer the valid request that they applied to
    #how many skills they have that match that request and the request is in their range. Ordered by most matching skills
    #to least. This is to help volunteer find which reqest they are most suited/helpful in.
    view2 = ("""create or replace view most_suited_for as
                select v.id, v."name",  rl.request_id , count(concat(rl.request_id, v.id)) as suited_skill 
                from volunteer_application va, volunteer_range vr, request_location rl, request_skill rs, skill_assignment sa, volunteer v 
                where va.request_id = rl.request_id and va.volunteer_id = vr.volunteer_id and rl.city_id = vr.city_id and va.is_valid = true and rs.request_id = rl.request_id and 
                va.volunteer_id = sa.volunteer_id and rs.skill_name = sa.skill_name and va.volunteer_id = v.id 
                group by v.id, rl.request_id, v."name" 
                order by request_id desc, stuff desc 
                 """)
    cursor.execute(view2)

    # exercise d.1
    def d1():

        noFVolunteers=(pd.read_sql_query("""--number of volunteers available by city
        select c.name as city,count(v.name) as volunteer_amount
        from volunteer v join volunteer_range vr on v.id =vr.volunteer_id join city c on c.id =vr.city_id
        group by c.name
        order by c.name desc;""",conn))

        nofApplicant=pd.read_sql_query("""--amount of applicants by city
        select c.name  as city,count( distinct va.volunteer_id) as applicant_amount
        from request_location rl join volunteer_application va on va.request_id =rl.request_id
        join city c on c.id =rl.city_id 
        group by c.name
        order by c.name desc;""",conn)

        #dataframes merged by city
        merged=pd.merge(noFVolunteers,nofApplicant, on="city")
        #drawing graph with x-axis being the city and y-axis being the amount of volunteers/applicants
        fig, ax = plt.subplots(figsize=(10, 6))
        bar_width = 0.35
        index = range(len(merged))
        bar1 = plt.bar(index, merged['volunteer_amount'], bar_width, label='Volunteers')
        bar2 = plt.bar([i + bar_width for i in index], merged['applicant_amount'], bar_width, label='Applicants')

        plt.xlabel('City')
        plt.ylabel('Amount')
        plt.title('Volunteers vs Applicants by City')
        plt.xticks([i + bar_width / 2 for i in index], merged['city'])
        plt.legend()

        plt.tight_layout()
        plt.show()

        print("Top 2 cities with the most volunteers:")
        print(merged.nlargest(2, 'volunteer_amount')[['city', 'volunteer_amount']])

        print("\nBottom 2 cities with the least volunteers:")
        print(merged.nsmallest(2, 'volunteer_amount')[['city', 'volunteer_amount']])


    def d2():

        #creating dict of maximum values for points
        maxForSkillDivision=pd.read_sql_query("""select r.id ,sum(rs.value+1)*1.5 as max_importance
        from request r join request_skill rs on r.id =rs.request_id
        group by r.id;""",conn)
        dictForMax=maxForSkillDivision.set_index('id')['max_importance'].to_dict()


        #creating a dictionary of interests
        interests= pd.read_sql_query("SELECT ia.volunteer_id,ia.interest_name   FROM interest_assignment ia;",conn).to_dict('split',index=False)
        grouped_data = defaultdict(list)
        for volunteer_id, interest_name in interests['data']:
            grouped_data[volunteer_id].append(interest_name)
        grouped_data = dict(grouped_data)

       # print(grouped_data)

        applicantSkills=pd.read_sql_query("""SELECT r.id AS request, va.volunteer_id  ,rs.skill_name ,rs.value+1 AS skill_importance
            FROM request r JOIN request_skill rs ON r.id =rs.request_id
            JOIN volunteer_application va ON va.request_id =r.id
            JOIN skill_assignment sa ON sa.volunteer_id = va.volunteer_id 
            JOIN volunteer_range vr ON vr.volunteer_id =va.volunteer_id 
            JOIN request_location rl ON rl.request_id =r.id 
            WHERE sa.skill_name =rs.skill_name AND vr.city_id = rl.city_id 
            GROUP BY r.id, va.volunteer_id  ,rs.skill_name ,rs.value
            ORDER BY r.id, volunteer_id;""",conn)


        #doens't work, fix this, is supposed to multiply value by 1.5 if it is in interests
        #print(applicantSkills.groupby(['volunteer_id', 'request'])['skill_importance'].sum().reset_index()['skill_importance'].sum())

        applicantSkills['skill_importance'] = applicantSkills.apply(lambda row: (row['skill_importance'] * 1.5) if row['skill_name'] in grouped_data[row['volunteer_id']] else row['skill_importance'], axis=1)
       #should be more than 13536
        result = applicantSkills.groupby(['volunteer_id', 'request'])['skill_importance'].sum().reset_index()


        result['skill_importance'] = result.apply(lambda row: (row['skill_importance']/dictForMax[row['request']])*0.95, axis=1)


        travel_readiness=pd.read_sql_query("""SELECT v.id ,v.travel_readiness FROM volunteer v ORDER BY v.travel_readiness DESC;""",conn)
        maxTravel=(travel_readiness['travel_readiness'][0])
        minTravel=(travel_readiness['travel_readiness'].tail(1))

        #make a dic out of travel readiness points
        travel_readiness['travel_readiness']=travel_readiness.apply(lambda row: ((row['travel_readiness']-minTravel)/(maxTravel-minTravel))*0.05,axis=1)
        travelDict=travel_readiness.set_index('id')['travel_readiness'].to_dict()
        #add travel readiness points to total score
        result['skill_importance'] = result.apply(lambda row: (travelDict[row['volunteer_id']])+row['skill_importance'], axis=1)


        top_volunteers = result.groupby('request').apply(lambda x: x.nlargest(5, 'skill_importance')).reset_index(drop=True)
        #print(top_volunteers)

        #print top 5 candidates (if enough) for each request
        #Only candidates who have applied and are within range are considered.
        # the formula to calculate the match % is
        # (the importance of each skill they posess* 1.5 if it is an interest)/ maximum points possible this way *0.95
        # + 0.05 * travel readiness normalized between 0 and 1 based on the min and max score.

        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(top_volunteers)


    #d2()


    """
    For each month, what are the number of valid volunteer applications
    compared to the number of valid requests? What months have the most and
    least for each, how about the difference between the requests and
    volunteers for each month? Is there a general/seasonal trend? Is there any
    correlation between the time of the year and number of requests and
    volunteers
    """
    def BD3():
        nofRequests=pd.read_sql_query("""-- amount of request per month
        SELECT (EXTRACT (MONTH FROM r.start_date)) as month,count(r.id) AS amount_of_requests
        FROM request r
        GROUP BY EXTRACT (MONTH FROM r.start_date)
        ORDER BY EXTRACT (MONTH FROM r.start_date) asc;""",conn)

        nofApplications = pd.read_sql_query("""-- amount of applications
        SELECT EXTRACT (MONTH FROM va.modified) as month,count(va.id) as amount_of_applications
        FROM volunteer_application va
        WHERE va.is_valid
        GROUP BY EXTRACT(MONTH FROM va.modified)
        ORDER BY EXTRACT(MONTH FROM va.modified);""",conn)

        #print(nofRequests)
        #print(nofApplications)


        # Plotting
        plt.figure(figsize=(10, 6))
        plt.plot(nofRequests['month'], nofRequests['amount_of_requests'], marker='o', linestyle='-', color='b')

        plt.title('Number of Requests per Month')
        plt.xlabel('Month')
        plt.ylabel('Amount of Requests')
        plt.xticks(range(1, 13))
        plt.grid(True)

        plt.figure(figsize=(10, 6))
        plt.plot(nofApplications['month'], nofApplications['amount_of_applications'], marker='o', linestyle='-', color='b')

        plt.title('Number of Applications per Month')
        plt.xlabel('Month')
        plt.ylabel('Amount of Applications')
        plt.xticks(range(1, 13))
        plt.grid(True)

        plt.figure(figsize=(10, 6))
        plt.plot(nofRequests['month'], (nofApplications['amount_of_applications']-nofRequests['amount_of_requests']), marker='o', linestyle='-', color='b')

        plt.title('Difference between the amount of requests and applications')
        plt.xlabel('Month')
        plt.ylabel('Difference')
        plt.xticks(range(1, 13))
        plt.grid(True)

        plt.show
        print(f"Correlation between amount of requests and month: {nofRequests['amount_of_requests'].corr(nofRequests['month']).round(4)}")
        print(f"Correlation between amount of applications and month: {nofApplications['amount_of_applications'].corr(nofRequests['month']).round(4)}")

        #From the correlation coefficients we can see that there is some linear correlation between the month and amount of requests.
        #However based on the graphs shown we can also see that there amount of applications almost follows a normal curve, peaking during the summer months.
        #The amount of requests also follow this albeit a bit less.
        # A strong reasonal trend is that during the summer there are more apllications and requests, likely due to the summer vacations most insitutions have
        # When people are working they have less time to spend on volunteering.

  #  Ad3()


    #Is there a correlation between the amount of skills and distance?
    #What about the amount of skills and amount of applications?
    def BD4():
        amount_of_skills=pd.read_sql_query("SELECT v.name ,v.travel_readiness ,count(sa.skill_name) FROM volunteer v JOIN skill_assignment sa ON sa.volunteer_id =v.id GROUP BY v.name,v.travel_readiness  ORDER BY count(sa.skill_name) DESC;",conn)
        plt.figure(figsize=(10, 6))
        plt.scatter(amount_of_skills['count'], amount_of_skills['travel_readiness'], marker='o', linestyle='-', color='b')

        plt.title('Travel readiness in relation to the amount of skills of a volunteer')
        plt.xlabel('Amount of skills')
        plt.ylabel('Travel readiness')
        plt.xticks(range(1, 15))
        plt.grid(True)
        #Based off the chart we can see that there is no real correlation between the amount of skills a person has and the amount they are willing to travel.

        amount_of_applications= pd.read_sql_query("""SELECT v."name" ,count (va.id) AS amount_of_applications,a.scount
                                                     FROM volunteer v 
                                                     JOIN volunteer_application va ON va.volunteer_id =v.id 
                                                     JOIN (SELECT v."name" AS id,count(sa.skill_name) AS scount FROM volunteer v JOIN skill_assignment sa ON sa.volunteer_id =v.id GROUP BY v.name) AS a ON a.id=v."name" 
                                                     GROUP BY v.name,a.scount;""",conn)

        plt.figure(figsize=(10, 6))
        plt.scatter(amount_of_applications['scount'],amount_of_applications['amount_of_applications'])

        plt.title('Amount of skills in relation to the amount of applications by volunteer')
        plt.xlabel('Amount of skills')
        plt.ylabel('Amount of applications')
        plt.xticks(range(1, 15))
        plt.grid(True)
        plt.show()

        print(amount_of_applications['scount'].corr(amount_of_applications['amount_of_applications']))
        #There is no discernable correlation between the amount of skills and the amount of applications either.
        #Though when a volunteer has 7,8 or 9 skills, it seems the average amount of requests is a bit higher according to the graph
    BD4()

    # Exercise B.b.1
    cursor.execute(
        """
            CREATE OR REPLACE FUNCTION validate_id(id TEXT) 
            RETURNS BOOLEAN AS $$
            DECLARE
                dob TEXT;
                individualized_part TEXT;
                full_number TEXT;
                control_character TEXT;
                valid_seperators TEXT[] := '{ +, -, A, B, C, D, E, F, X, Y, W, V, U }';
                valid_control_characters TEXT[] := '{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, B, C, D, E, F, H, J, K, L, M, N, P, R, S, T, U, V, W, X, Y }';
                calculated_control_character TEXT;
                remainder INTEGER;
            BEGIN
                -- Return false if id is not 11 characters long
                IF (id NOT LIKE '___________') THEN
                    RETURN FALSE;
                END IF;

                -- Return false f 7th character is not one of the allowed ones
                IF NOT (SUBSTRING(id FROM 7 FOR 1) = ANY(valid_seperators)) THEN
                    RETURN FALSE;
                END IF;

                -- Assign variables
                dob := SUBSTRING(id FROM 1 FOR 6);
                individualized_part := SUBSTRING(id FROM 8 FOR 3);
                control_character := SUBSTRING(id FROM 11 FOR 1);

                -- Calculate the control character
                full_number := CONCAT(dob, individualized_part);
                remainder := CAST(full_number AS INTEGER) % 31;
                calculated_control_character := valid_control_characters[remainder + 1];

                -- Check if it's the correct control character or not
                IF calculated_control_character != control_character THEN
                    RETURN FALSE;
                END IF;

                RETURN TRUE;
            END;
            $$ LANGUAGE plpgsql;

            -- This line is for testing purposes
            ALTER TABLE volunteer DROP CONSTRAINT IF EXISTS check_id;

            -- Add the constraint to volunteer
            ALTER TABLE volunteer ADD CONSTRAINT check_id CHECK (validate_id(id));
        """
    )

    # Exercise B.b.2
    # For this one, instead of using the provided formula, the new number of volunteers is calculated by adding the previous one with the difference between the new and previous min_need. The 2 formulas should be equivalent.
    cursor.execute(
        """
        CREATE OR REPLACE FUNCTION update_number_of_volunteers() RETURNS TRIGGER AS $$
        DECLARE 
            previous_value INT;
        BEGIN
            previous_value := (SELECT number_of_volunteers FROM request WHERE id = OLD.request_id);
            UPDATE request SET number_of_volunteers = previous_value + NEW.min_need - OLD.min_need WHERE id = OLD.request_id;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE TRIGGER update_no_of_volunteers
        AFTER UPDATE OF min_need ON request_skill
        FOR EACH ROW
        EXECUTE FUNCTION update_number_of_volunteers();
        """
    )

    # Exercise B.c.1 (transaction)
    """
    The specification is extremely unclear and contradictory, so this is how I interpret and implement it:
    """
    cursor.execute(
        """
        CREATE TABLE volunteer_assignment (
            request_id INT,
            volunteer_id TEXT,
            PRIMARY KEY (request_id, volunteer_id)
        );

        CREATE OR REPLACE FUNCTION assign_volunteers(req_id INT)
        RETURNS VOID AS $$
        DECLARE
            skill RECORD;
            application RECORD;
            total_assigned INT := 0;
            skill_assigned INT := 0;
            min_volunteers INT := 0;
            v_count INT;
            register_by_date DATE;
        BEGIN
            register_by_date := (SELECT request.register_by_date FROM request WHERE id = req_id); 

            -- Get the minimum number of volunteers needed for the request
            SELECT SUM(min_need) INTO min_volunteers
            FROM request_skill r
            WHERE r.request_id = req_id;

            -- Step 1: Assign volunteers based on skills
            FOR skill IN
                SELECT * FROM request_skill r WHERE r.request_id = req_id ORDER BY value DESC
            LOOP
                skill_assigned := 0;

                FOR application IN
                    SELECT * FROM volunteer_application a
                    JOIN skill_assignment sa ON a.volunteer_id = sa.volunteer_id
                    WHERE a.request_id = req_id AND a.is_valid AND sa.skill_name = skill.skill_name
                    LIMIT skill.min_need
                LOOP
                    -- Assign the volunteer
                    INSERT INTO volunteer_assignment (request_id, volunteer_id) VALUES (req_id, application.volunteer_id)
                    ON CONFLICT DO NOTHING;

                    skill_assigned := skill_assigned + 1;
                    total_assigned := total_assigned + 1;

                    -- Exit if the minimum required volunteers are met for this skill
                    EXIT WHEN skill_assigned >= skill.min_need;
                END LOOP;
            END LOOP;

            -- Step 2: Assign remaining volunteers
            FOR application IN
                SELECT * FROM volunteer_application a WHERE a.request_id = req_id AND is_valid
            LOOP
                -- Assign the volunteer
                INSERT INTO volunteer_assignment (request_id, volunteer_id) VALUES (req_id, application.volunteer_id)
                ON CONFLICT DO NOTHING;

                total_assigned := total_assigned + 1;
            END LOOP;

            -- Check conditions for commit or rollback
            IF total_assigned < min_volunteers THEN
                IF register_by_date > CURRENT_DATE THEN
                    -- Rollback if the registration date is not past and minimum volunteers are not met
                    ROLLBACK;
                ELSE
                    -- If the registration date is past and minimum volunteers are not met, extend registration or accept volunteers. the time 1 week is arbitrarily picked because the specification did not specify the duration.  
                    UPDATE request SET register_by_date = register_by_date + INTERVAL '7 days' WHERE id = req_id;
                END IF;
            ELSE
            END IF;
        END;
        $$ LANGUAGE plpgsql;

        -- run the transaction on a random request
        SELECT assign_volunteers(1);

        -- see the results
        SELECT * FROM volunteer_assignment;
        """
    )

    # Exercise B.c.2 (transaction)
    """
        This transaction register a new volunteer, assign their skills, and set their range.
        I think this transaction should be implemented because registering a new volunteer is a crucial step that will be needed in the future.
        In production, this transaction can take in the volunteers' inputs as parameters for their skills and ranges.
    """
    cursor.execute(
        """
        BEGIN;

        INSERT INTO volunteer (id, birthdate, city_id, "name", email, address, travel_readiness) VALUES ('111111-111C', '11/11/1911', '072', 'John Doe', 'john.doe@gmail.com', 'Böstaksentie 4, Hailuoto', '704');
        INSERT INTO skill_assignment (volunteer_id, skill_name) VALUES ('111111-111C', 'TrainPeople'), ('111111-111C', 'Rescue');
        INSERT INTO volunteer_range (volunteer_id, city_id) VALUES ('111111-111C', '072');

        COMMIT;
        """
    )

    records = cursor.fetchall()
    for record in records:
        print(record)



except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")