    CREATE TABLE IF NOT EXISTS CarePlan (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        ActualReleaseDate DATE,
        NameOfYouth VARCHAR(255),
        RaceEthnicity VARCHAR(100),
        MediCalID VARCHAR(50),
        ResidentialAddress TEXT,
        Telephone VARCHAR(20),
        MediCalHealthPlan VARCHAR(100),
        HealthScreenings TEXT,
        HealthAssessments TEXT,
        ChronicConditions TEXT,
        PrescribedMedications TEXT,
        Notes TEXT,
        CarePlanNotes TEXT,
        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    """)

    care_plan_data = []

    youth_names = ['James Smith', 'Maria Garcia', 'David Johnson', 'Sarah Williams', 'Michael Brown',
                   'Jessica Davis', 'Christopher Miller', 'Ashley Wilson', 'Matthew Moore', 'Amanda Taylor',
                   'Joshua Anderson', 'Jennifer Thomas', 'Daniel Jackson', 'Elizabeth White', 'Andrew Harris',
                   'Michelle Martin', 'Anthony Thompson', 'Emily Garcia', 'Robert Martinez', 'Nicole Rodriguez']

    races = ['Caucasian', 'African American', 'Hispanic/Latino', 'Asian', 'Native American',
             'Pacific Islander', 'Multiracial', 'Other']

    health_plans = ['Anthem Blue Cross', 'Kaiser Permanente', 'Health Net', 'Blue Shield of California',
                    'Molina Healthcare', 'LA Care Health Plan', 'Community Health Group']

    chronic_conditions_options = ['Asthma', 'Diabetes', 'Hypertension', 'Depression', 'Anxiety', 'ADHD',
                                  'Bipolar Disorder', 'PTSD', 'Substance Use Disorder', 'None']

    medications_options = ['Metformin', 'Lisinopril', 'Albuterol', 'Sertraline', 'Escitalopram',
                           'Bupropion', 'Risperidone', 'Insulin', 'None']

    careplan_note_templates = [
        "{name} demonstrates exceptional engagement in cognitive behavioral therapy sessions, showing 70% improvement in emotional regulation skills. Family therapy sessions with parents have strengthened communication patterns. Current medication regimen for {condition} is well-tolerated with minimal side effects.",
        "{name} has successfully maintained stable housing with supportive family network for 6 months. Educational goals include completing GED program by December 2025. Mental health services include weekly individual therapy and bi-weekly psychiatric consultations.",
        "{name} is actively participating in outpatient substance use treatment program with 45 days of sobriety maintained. Regular attendance at AA/NA meetings (4x weekly) demonstrates strong commitment to recovery. Peer mentor relationship established with program alumni.",
        "Medical follow-ups for {name} show consistent attendance at appointments. {condition} management has improved with current treatment protocol. Family communication has strengthened through structured therapy sessions and conflict resolution skills training.",
        "{name} exhibits strong motivation toward positive behavioral changes, evidenced by completion of anger management program. Educational support services include tutoring and vocational training enrollment. Transportation barriers resolved through public transit training program.",
        "Therapy progress for {name} includes development of healthy coping mechanisms and stress management techniques. Community resource integration successful including enrollment in recreational activities and volunteer opportunities. Family involvement has increased significantly.",
        "{name} is responding positively to trauma-informed care approach with measurable progress in PTSD symptoms reduction. Recent assessment shows 60% decrease in anxiety episodes. Social skills development through group therapy sessions shows marked improvement.",
        "Case management for {name} has identified multiple strengths including leadership potential and artistic abilities. Family reunification process is progressing well with supervised visits increasing to unsupervised weekend stays.",
        "{name} faces ongoing challenges with medication adherence; pill organizer system and reminder apps have been implemented successfully. Support system includes mentor, case worker, and peer support specialist meeting weekly.",
        "Treatment team has recognized {name}'s exceptional progress in developing independent living skills and financial literacy. Career counseling has identified interests in healthcare field with plans for CNA training enrollment."
    ]

    for i in range(100):
        youth_name = random.choice(youth_names)
        selected_condition = random.choice(
            ['Anxiety', 'Depression', 'ADHD', 'PTSD', 'Bipolar Disorder']) if random.choice(
            [True, False]) else 'ongoing treatment'

        # Create personalized note
        note_template = random.choice(careplan_note_templates)
        careplan_note = note_template.format(name=youth_name.split()[0],
                                             condition=selected_condition)  # Using first name only

        care_plan_data.append((
            datetime.now().date() - timedelta(days=random.randint(0, 730)),
            youth_name,
            random.choice(races),
            f"MC{random.randint(1000000, 9999999)}",
            f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Maple', 'Pine'])} St, "
            f"{random.choice(['Los Angeles', 'San Diego', 'San Francisco', 'Sacramento', 'Oakland'])} CA",
            f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            random.choice(health_plans),

            f"Last screening: {random.choice(['2023', '2024'])}; "
            f"Results: {random.choice(['Normal', 'Abnormal - follow up needed', 'Pending'])}",

            f"Comprehensive assessment completed; Risk level: {random.choice(['Low', 'Medium', 'High'])}",

            ", ".join(random.sample([c for c in chronic_conditions_options if c != 'None'],
                                    random.randint(0, 2))) or 'None',

            ", ".join(random.sample([m for m in medications_options if m != 'None'],
                                    random.randint(0, 3))) or 'None',

            f"Youth shows {random.choice(['excellent', 'good', 'fair', 'poor'])} progress in treatment. "
            f"Primary focus areas: {random.choice(['education', 'family relationships', 'mental health', 'substance use'])}. "
            f"Strengths: {random.choice(['motivation', 'family support', 'academic ability', 'resilience'])}.",

            careplan_note
        ))

    sql_cur.executemany("""
        INSERT INTO CarePlan (ActualReleaseDate, NameOfYouth, RaceEthnicity, MediCalID, ResidentialAddress, 
                             Telephone, MediCalHealthPlan, HealthScreenings, HealthAssessments, 
                             ChronicConditions, PrescribedMedications, Notes, CarePlanNotes
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, care_plan_data)
