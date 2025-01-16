import re

def chinese_medical_university_affs(aff_string, current_affs, aff_id_1, one_off_affs, affs_to_add, 
                                    strings_for_affs):
    
    all_affs_to_search = [aff_id_1]+one_off_affs+affs_to_add
    
    if any(aff_id in current_affs for aff_id in all_affs_to_search):
        current_affs.append(aff_id_1)
        for aff_id_check, aff_string_check in zip(affs_to_add, strings_for_affs):
            if aff_id_check in current_affs:
                for aff_id_check_2, aff_string_check_2 in zip(affs_to_add, strings_for_affs):
                    if aff_id_check_2 == aff_id_check:
                        pass
                    else:
                        if aff_string_check_2 in aff_string:
                            if aff_id_check in current_affs:
                                current_affs.remove(aff_id_check)
                            if aff_id_check_2 not in current_affs:
                                current_affs.append(aff_id_check_2)
    return current_affs

def matching_based_on_current_affs(current_affs, aff_string):
    if (len(current_affs) > 1) & (-1 in current_affs):
        current_affs.remove(-1)

    # Sorbonne Hospitals
    if any(inst in current_affs for inst in [4210086685,4210166768,4210134887,4210153132,4210090185,
                                             4210102928,4210121705]):
        current_affs.append(39804081)

    # International Council for the Exploration of the Sea
    if 282179226 in current_affs:
        if 'denmark' not in aff_string.lower():
            if 'sea' not in aff_string.lower():
                current_affs.remove(282179226)

    # American Institutes for Research
    if 1293631320 in current_affs:
        if 'ethical approval' in aff_string.lower():
            current_affs.remove(1293631320)

    # # Amsterdam University Medical Center
    # if 4210151833 in current_affs:
    #     if 865915315 in current_affs:
    #         current_affs.remove(865915315)

    # Anton Pannekoek Institute for Astronomy
    if 2898336195 in current_affs:
        if 'anton pannekoek' in aff_string.lower():
            current_affs.remove(2898336195)

    # Chemin de Polytechnique
    if 45683168 in current_affs:
        if 'chemin' in aff_string.lower():
            if any(word in aff_string for word in ['2940','2950','2017']):
                current_affs.remove(45683168)
    
    # Helen Hay Whitney Foundation
    if 262162183 in current_affs:
        if 'Helen' not in aff_string:
            current_affs.remove(262162183)
            current_affs.append(32971472)
    
    # Artificial Intelligence Research Institute
    if 4210131846 in current_affs:
        if 'airi' in aff_string.lower():
            current_affs.remove(4210131846)
            current_affs.append(4392021246)
        elif 'moscow' in aff_string.lower():
            current_affs.remove(4210131846)
            current_affs.append(4392021246)

    # Valleywise Health
    if 4210152461 in current_affs:
        if 'MIHS' not in aff_string:
            if 'health' not in aff_string.lower():
                if 'integrated' not in aff_string.lower():
                    current_affs.remove(4210152461)

    # California Academy of Sciences
    if 2803094215 in current_affs:
        if 'mathematics' in aff_string.lower():
            current_affs.remove(2803094215)

    # Manado State University
    if 3131162304 in current_affs:
        if 'UNIMA' not in aff_string:
            if 'negeri' not in aff_string.lower():
                if 'state' not in aff_string.lower():
                    current_affs.remove(3131162304)

    # Institut Català de Nanociència i Nanotecnologia
    if 4210093216 in current_affs:
        if re.search('\\bIN2UB\\b', aff_string):
            current_affs.append(4401200369)
            current_affs.remove(4210093216)
        elif any(word in aff_string.lower() for word in ["universitat de barcelona","university of barcelona"]):
            current_affs.append(4401200369)
            current_affs.remove(4210093216)

    # Department of Agriculture and Fisheries, Queensland Government
    if 2801244131 in current_affs:
        if 'Department of Agriculture and Fisheries, Queensland Government' in aff_string:
            current_affs.remove(2801244131)
            current_affs.append(4210164589)

    # St Xavier's in India
    if 906608882 in current_affs:
        if any(word in aff_string
               for word in ["St. Xavier's University", "St. Xaviers University",
                            "St Xaviers University", "St Xavier's University"]):
            if 'Kolkata' in aff_string:
                current_affs.append(4400573289)
                current_affs.remove(906608882)

    # Helsinki Art Museum
    if 4210102852 in current_affs:
        if 'elsingin yliopisto' in aff_string:
            current_affs.append(133731052)
            current_affs.remove(4210102852)

    # IIM
    if 150870154 in current_affs:
        if 'Bodhgaya' in current_affs:
            current_affs.append(4400600926)
            current_affs.remove(150870154)

    # Concordia University
    if 105925353 in current_affs:
        if any(word in aff_string.lower() for word in ["université concordia", "universite concordia",
                                                        "québec", "montréal", "quebec", "montreal",
                                                         "h3g ", "h3g1m8", "maisonneuve"]):
            current_affs.append(60158472)
            current_affs.remove(105925353)

    # University of Arizona
    if 4210122332 in current_affs:
        if any(word in aff_string.lower() for word in ['optical sciences center', 'college of optical sciences', 'wyant']):
            current_affs.remove(4210122332)
            current_affs.append(138006243)

    if 4210166658 in current_affs:
        if 'university of arizona' in aff_string.lower():
            current_affs.remove(4210166658)
            current_affs.append(138006243)

    # Bari fixes
    if 68618741 in current_affs:
        if any(word in aff_string for word in ['Technical University of Bari','Technical Univ. of Bari',
                                            'University and Politecnico of Bari',
                                            'Technical University Politecnico di Bari',
                                            'Polytechnic University of Bari',
                                            'Politecnico di Bari',
                                            'Polytechnic of Bari',
                                            'Univ. and Politecnico of Bari','Polytechinic University of Bari']):
            pass
        else:
            current_affs.remove(68618741)

    # Institut Polytechnique de Paris
    if 4210145102 in current_affs:
        if any(word in aff_string for word in ['Telecom SudParis','Télécom SudParis','Telecom SudParís',
                                               'TELECOM SudParis','Telecom-SudParis','Telecom Sudparis',
                                               'Télécom Sud Paris','Telecom SudParis','IPParis']):
            pass
        elif re.search('\\bIPP\\b', aff_string):
            if any(word in aff_string.lower() for word in ['palaiseau','paris']):
                pass
            else:
                current_affs.remove(4210145102)
        else:
            current_affs.remove(4210145102)

    # Adana Hospital
    if 4210094594 in current_affs:
        if any(word in aff_string.lower() for word in ['başkent','baskent','university hospital']):
            pass
        else:
            current_affs.remove(4210094594)

    # Amity Univ Noida
    if 191972202 in current_affs:
        if any(word in aff_string for word in ['Noida']):
            pass
        else:
            current_affs.remove(191972202)

    # Natl Inst Technol - Trichy
    if 122964287 in current_affs:
        if any(word in aff_string for word in ['National Engineering College']):
            if 'Kovilpatti' in aff_string:
                current_affs.remove(122964287)
        elif 'National College' in aff_string:
            if 'Tiruchirappalli' in aff_string:
                if not re.search('\\bNIT\\b', aff_string):
                    if 'National Institute' not in aff_string:
                        current_affs.remove(122964287)
        else:
            pass

    # Afyonkarahisar Health Sciences University and Kutahya Health Sciences University
    if 4210128276 in current_affs:
        if 'Afyonkarahisar Health Sciences University' in aff_string:
            current_affs.remove(4210128276)
            current_affs.append(4387154071)
        elif 'Kutahya Health Sciences University' in aff_string:
            current_affs.remove(4210128276)
            current_affs.append(4387156457)
        else:
            pass

    # Islamic Azad University
    if any(aff in current_affs for aff in [155419210, 110525433]):
        # Islamic Azad University Rasht Branch
        if 'Rasht' in aff_string:
            current_affs.append(4210098966)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Mahabad
        elif 'Mahabad' in aff_string:
            current_affs.append(4210101274)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Ahvaz Branch
        elif 'Ahvaz' in aff_string:
            current_affs.append(4210147666)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Zahedan Branch
        elif 'Zahedan' in aff_string:
            current_affs.append(4210103813)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Mashhad
        elif 'Mashhad' in aff_string:
            current_affs.append(183859904)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, UAE Branch
        elif 'UAE' in aff_string:
            current_affs.append(4210124555)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Naragh Branch
        elif 'Naragh' in aff_string:
            current_affs.append(4210124802)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Karaj
        elif 'Karaj' in aff_string:
            current_affs.append(204588832)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Falavarjan
        elif 'Falavarjan' in aff_string:
            current_affs.append(2802842351)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Shabestar
        elif 'Shabestar' in aff_string:
            current_affs.append(4210130889)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Qazvin Islamic Azad University
        elif 'Qazvin' in aff_string:
            current_affs.append(197220011)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Semnan
        elif 'Semnan' in aff_string:
            current_affs.append(4210104301)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Azadshahr Branch
        elif 'Azadshahr' in aff_string:
            current_affs.append(4210147006)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University North Tehran Branch
        elif 'North Tehran' in aff_string:
            current_affs.append(183067279)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Firoozkooh Branch
        elif 'Firoozkooh' in aff_string:
            current_affs.append(4210103782)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Ayatollah Amoli
        elif 'Ayatollah Amoli' in aff_string:
            current_affs.append(4210116557)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Sanandaj Branch
        elif 'Sanandaj' in aff_string:
            current_affs.append(2801503745)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Damghan Branch
        elif 'Damghan' in aff_string:
            current_affs.append(4210159640)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Dehaghan
        elif 'Dehaghan' in aff_string:
            current_affs.append(4210106588)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Qaemshahr Islamic Azad University
        elif 'Qaemshahr' in aff_string:
            current_affs.append(4210138903)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Islamshahr Branch
        elif 'Islamshahr' in aff_string:
            current_affs.append(4210159652)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Damavand
        elif 'Damavand' in aff_string:
            current_affs.append(4210163293)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Birjand
        elif 'Birjand' in aff_string:
            current_affs.append(4210122540)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Maybod
        elif 'Maybod' in aff_string:
            current_affs.append(4210145406)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Shoushtar Branch
        elif 'Shoushtar' in aff_string:
            current_affs.append(4210153432)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Kerman
        elif 'Kerman' in aff_string:
            current_affs.append(4210152146)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Larestan Branch
        elif 'Larestan' in aff_string:
            current_affs.append(4210145150)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Shirvan Branch
        elif 'Shirvan' in aff_string:
            current_affs.append(4210120980)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Pharmaceutical Sciences Branch
        elif 'Pharmaceutical Sciences' in aff_string:
            current_affs.append(4210152512)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Science and Research Branch
        elif 'Science and Research' in aff_string:
            current_affs.append(155419210)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Marvdasht
        elif 'Marvdasht' in aff_string:
            current_affs.append(4210090965)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Shahrekord
        elif 'Shahrekord' in aff_string:
            current_affs.append(4210121772)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Sari Branch
        elif 'Sari' in aff_string:
            current_affs.append(4210086811)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Buin-Zahra
        elif 'Buin-Zahra' in aff_string:
            current_affs.append(4210123837)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Firuzabad Branch
        elif 'Firuzabad' in aff_string:
            current_affs.append(4210095796)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Omidieh Branch
        elif 'Omidieh' in aff_string:
            current_affs.append(4210127412)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Jiroft Branch
        elif 'Jirt' in aff_string:
            current_affs.append(4210122456)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Gorgan
        elif 'Gorgan' in aff_string:
            current_affs.append(4210117791)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Yasuj
        elif 'Yasuj' in aff_string:
            current_affs.append(4210126091)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Tonekabon
        elif 'Tonekabon' in aff_string:
            current_affs.append(4210149299)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Nishapur
        elif 'Nishapur' in aff_string:
            current_affs.append(4210113978)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Shiraz
        elif 'Shiraz' in aff_string:
            current_affs.append(91138267)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Sirjan Branch
        elif 'Sirjan' in aff_string:
            current_affs.append(4210137408)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Nowshahr Branch
        elif 'Nowshahr' in aff_string:
            current_affs.append(4210118876)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Gachsaran
        elif 'Gachsaran' in aff_string:
            current_affs.append(4210093963)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Sepidan
        elif 'Sepidan' in aff_string:
            current_affs.append(4387152370)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Majlesi
        elif 'Majlesi' in aff_string:
            current_affs.append(261916979)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Central Tehran Branch
        elif 'Central Tehran' in aff_string:
            current_affs.append(41775361)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Masjed Soleyman
        elif 'Masjed Soleyman' in aff_string:
            current_affs.append(4210158817)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Malayer Branch
        elif 'Malayer' in aff_string:
            current_affs.append(4210131670)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Marand
        elif 'Marand' in aff_string:
            current_affs.append(4210151143)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Kashmar Branch
        elif 'Kashmar' in aff_string:
            current_affs.append(4210088567)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Tabriz
        elif 'Tabriz' in aff_string:
            current_affs.append(1293555014)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Kermanshah
        elif 'Kermanshah' in aff_string:
            current_affs.append(2801954088)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Dolatabad
        elif 'Dolatabad' in aff_string:
            current_affs.append(4210140966)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Dental Branch of Tehran
        elif 'Dental   Tehran' in aff_string:
            current_affs.append(2801553314)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Shahr-e-Qods Branch
        elif 'Shahr-e-Qods' in aff_string:
            current_affs.append(4210116438)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Bonab Branch
        elif 'Bonab' in aff_string:
            current_affs.append(4210156805)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Varamin
        elif 'Varamin' in aff_string:
            current_affs.append(4210109236)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Fasa Branch
        elif 'Fasa' in aff_string:
            current_affs.append(4210108082)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Khorramabad Branch
        elif 'Khorramabad' in aff_string:
            current_affs.append(4210129655)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Arsanjan
        elif 'Arsanjan' in aff_string:
            current_affs.append(4210146840)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Ardabil
        elif 'Ardabil' in aff_string:
            current_affs.append(4210161716)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Mobarakeh
        elif 'Mobarakeh' in aff_string:
            current_affs.append(4210103000)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Khomeynishahr
        elif any(word in aff_string for word in ['Khomeinishahr', 'Khomeynishahr']):
            current_affs.append(2799360687)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Mahshahr
        elif 'Mahshahr' in aff_string:
            current_affs.append(189748745)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Isfahan
        elif 'Isfahan' in aff_string:
            current_affs.append(2799282979)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Malard
        elif 'Malard' in aff_string:
            current_affs.append(3010108046)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Parand
        elif 'Parand' in aff_string:
            current_affs.append(2802594446)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Najafabad
        elif 'Najafabad' in aff_string:
            current_affs.append(9256017)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Lahijan Branch
        elif 'Lahijan' in aff_string:
            current_affs.append(33162209)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Takestan Islamic Azad University
        elif 'Takestan' in aff_string:
            current_affs.append(4210165322)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Chalous
        elif 'Chalous' in aff_string:
            current_affs.append(4210118335)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Bandar Abbas
        elif 'Bandar Abbas' in aff_string:
            current_affs.append(4210110333)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Roudehen Branch
        elif 'Roudehen' in aff_string:
            current_affs.append(1306291479)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Zanjan
        elif 'Zanjan' in aff_string:
            current_affs.append(131837588)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Izeh Islamic Azad University
        elif 'Izeh' in aff_string:
            current_affs.append(4210144160)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Quchan
        elif 'Quchan' in aff_string:
            current_affs.append(4210152523)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University South Tehran Branch
        elif 'South Tehran' in aff_string:
            current_affs.append(136830121)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Arak
        elif 'Arak' in aff_string:
            current_affs.append(2801771032)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Boroujerd Branch
        elif 'Boroujerd' in aff_string:
            current_affs.append(4210114866)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Tehran
        elif 'Tehran' in aff_string:
            current_affs.append(110525433)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Farahan
        elif 'Farahan' in aff_string:
            current_affs.append(2802465448)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Hamedan
        elif 'Hamedan' in aff_string:
            current_affs.append(4210089026)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Kashan
        elif 'Kashan' in aff_string:
            current_affs.append(2801582476)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Bostanabad
        elif 'Bostanabad' in aff_string:
            current_affs.append(4210155841)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Shahreza
        elif 'Shahreza' in aff_string:
            current_affs.append(4210094515)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Khoy Branch
        elif 'Khoy' in aff_string:
            current_affs.append(4210124016)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Medical Branch of Tehran
        elif 'Medical   Tehran' in aff_string:
            current_affs.append(2800744764)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Tafresh
        elif 'Tafresh' in aff_string:
            current_affs.append(4210120974)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Urmia
        elif 'Urmia' in aff_string:
            current_affs.append(4210163840)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Aliabad Katoul
        elif 'Aliabad Katoul' in aff_string:
            current_affs.append(4210111814)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Yazd
        elif 'Yazd' in aff_string:
            current_affs.append(4210159272)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Babol
        elif 'Babol' in aff_string:
            current_affs.append(4400573191)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Garmsar
        elif 'Garmsar' in aff_string:
            current_affs.append(115420810)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Bandar Anzali Branch
        elif 'Bandar Anzali' in aff_string:
            current_affs.append(4210126954)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Komijan Branch
        elif 'Komijan' in aff_string:
            current_affs.append(4210115839)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Sabzevar
        elif 'Sabzevar' in aff_string:
            current_affs.append(4210140773)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Shahr-e-Rey
        elif 'Shahr-e-Rey' in aff_string:
            current_affs.append(4210154044)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Ahar
        elif 'Ahar' in aff_string:
            current_affs.append(4210112351)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Saveh
        elif 'Saveh' in aff_string:
            current_affs.append(4210162396)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Kazeron
        elif 'Kazeron' in aff_string:
            current_affs.append(4210125694)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Janah
        elif 'Janah' in aff_string:
            current_affs.append(4210154612)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Dezful Branch
        elif 'Dezful' in aff_string:
            current_affs.append(4210155764)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Ashtian Branch
        elif 'Ashtian' in aff_string:
            current_affs.append(4210106052)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Bushehr Branch
        elif 'Bushehr' in aff_string:
            current_affs.append(4210132474)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Abhar Branch
        elif 'Abhar' in aff_string:
            current_affs.append(4210110389)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Qom Islamic Azad University
        elif 'Qom' in aff_string:
            current_affs.append(4210113955)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Shahrood
        elif 'Shahrood' in aff_string:
            current_affs.append(4210093808)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Jahrom Branch
        elif 'Jahrom' in aff_string:
            current_affs.append(4210108307)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Mehriz
        elif 'Mehriz' in aff_string:
            current_affs.append(4210096727)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Boukan
        elif 'Boukan' in aff_string:
            current_affs.append(4210091465)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Bojnourd Branch
        elif 'Bojnourd' in aff_string:
            current_affs.append(93979751)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Estahban Branch
        elif 'Estahban' in aff_string:
            current_affs.append(4210090247)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Langarud Branch
        elif 'Langarud' in aff_string:
            current_affs.append(4210139362)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Shahinshahr Branch
        elif 'Shahinshahr' in aff_string:
            current_affs.append(4210157053)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Ilam Branch
        elif 'Ilam' in aff_string:
            current_affs.append(4210165053)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

    # AIIMS (All India Institute of Medical Sciences)
    if 63739035 in current_affs:
        # All India Institute of Medical Sciences Bhubaneswar
        if 'Bhubaneswar' in aff_string:
            current_affs.append(4210117092)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Bhopal
        elif 'Bhopal' in aff_string:
            current_affs.append(4210106490)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences, Nagpur
        elif 'Nagpur' in aff_string:
            current_affs.append(4401200305)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Guwahati
        elif 'Guwahati' in aff_string:
            current_affs.append(4387153078)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Rishikesh
        elif 'Rishikesh' in aff_string:
            current_affs.append(4387152206)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Raipur
        elif 'Raipur' in aff_string:
            current_affs.append(129734738)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Jodhpur
        elif 'Jodhpur' in aff_string:
            current_affs.append(216021267)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences, Deoghar
        elif 'Deoghar' in aff_string:
            current_affs.append(4396570500)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

    # Tianjin Medical University General Hospital
    if 5740404 in current_affs:
        if 'Tianjin Medical University General Hospital' in aff_string:
            current_affs.append(2802534033)
            current_affs.remove(5740404)

    # National Kaohsiung University of Science and Technology
    if 192168892 in current_affs:
        if any(word in aff_string.lower() for word in ['kaohsiung university of sci',
                                                       'kaohsiung univ. of science and tech',
                                                       'kaohsiung university sciences and tech']):
            current_affs.remove(192168892)
            current_affs.append(4387154394)

    # Institut Polytechnique de Paris
    if 4210145102 in current_affs:
      if any(word in aff_string for word in ['Telecom SudParis','Télécom SudParis','Telecom SudParís',
                                              'TELECOM SudParis','Telecom-SudParis','Telecom Sudparis',
                                              'Télécom Sud Paris','Telecom SudParis','IPParis',
                                              'Institut Polytechnique de Paris']):
          pass
      elif re.search('\\bIPP\\b', aff_string):
          if any(word in aff_string.lower() for word in ['palaiseau','paris']):
              pass
      elif re.search('\\bIP Paris\\b', aff_string):
          pass
      else:
          current_affs.remove(4210145102)

    # AGH University of Science and Technology
    if any(inst in current_affs for inst in [686019]):
        if 4210163816 in current_affs:
            current_affs.remove(4210163816)

    # Changchun University of Technology
    if any(inst in current_affs for inst in [4385474403]):
        if 49232843 in current_affs:
            current_affs.remove(49232843)
        if 106645853 in current_affs:
            current_affs.remove(106645853)

    # China Medical University
    if any(inst in current_affs for inst in [184693016,4210126829]):
        if 91656880 in current_affs:
            current_affs.remove(91656880)
        if 91807558 in current_affs:
            current_affs.remove(91807558)

    # China University of Petroleum East China
    if any(inst in current_affs for inst in [4210162190]):
        if 204553293 in current_affs:
            current_affs.remove(204553293)

    # Chinese Academy of Medical Sciences Peking Union Medical College
    if any(inst in current_affs for inst in [200296433]):
        if 4210119648 in current_affs:
            current_affs.remove(4210119648)
        if 4210141683 in current_affs:
            current_affs.remove(4210141683)
        if 4210147708 in current_affs:
            current_affs.remove(4210147708)
        if 4210092004 in current_affs:
            current_affs.remove(4210092004)
        if 4210141458 in current_affs:
            current_affs.remove(4210141458)

    # Claude Bernard University Lyon 1
    if any(inst in current_affs for inst in [100532134]):
        if 203339264 in current_affs:
            current_affs.remove(203339264)

    # Cornell University
    if any(inst in current_affs for inst in [205783295,4210152471]):
        if 145220665 in current_affs:
            current_affs.remove(145220665)
        if 92528248 in current_affs:
            current_affs.remove(92528248)

    # Czech Technical University in Prague
    if any(inst in current_affs for inst in [44504214]):
        if 4210100395 in current_affs:
            current_affs.remove(4210100395)

    # Education University of Hong Kong
    if any(inst in current_affs for inst in [4210086892]):
        if 16518940 in current_affs:
            current_affs.remove(16518940)
        if 200769079 in current_affs:
            current_affs.remove(200769079)
        if 177725633 in current_affs:
            current_affs.remove(177725633)
        if 168719708 in current_affs:
            current_affs.remove(168719708)
        if 8679417 in current_affs:
            current_affs.remove(8679417)

    # Ohio University
    if any(inst in current_affs for inst in [4210106879]):
        if 22759111 in current_affs:
            current_affs.remove(22759111)

    # Poznan University of Life Sciences
    if any(inst in current_affs for inst in [55783418]):
        if 158552681 in current_affs:
            current_affs.remove(158552681)

    # Graz University of Technology
    if any(inst in current_affs for inst in [4092182]):
        if 15766117 in current_affs:
            current_affs.remove(15766117)

    # Heinrich Heine University Düsseldorf
    if any(inst in current_affs for inst in [44260953]):
        if 75356249 in current_affs:
            current_affs.remove(75356249)

    # Henan Polytechnic University
    if any(inst in current_affs for inst in [4210166499]):
        if 4210115515 in current_affs:
            current_affs.remove(4210115515)

    # Hubei University
    if any(inst in current_affs for inst in [75900474]):
        if 4210099437 in current_affs:
            current_affs.remove(4210099437)
        if 4210154851 in current_affs:
            current_affs.remove(4210154851)

    # Indian Institute of Technology Dhanbad
    if any(inst in current_affs for inst in [189109744]):
        if 64295750 in current_affs:
            current_affs.remove(64295750)

    # Indian Institute of Technology Hyderabad
    if any(inst in current_affs for inst in [65181880]):
        if 64189192 in current_affs:
            current_affs.remove(64189192)

    # Indiana University Purdue University Indianapolis
    if any(inst in current_affs for inst in [55769427]):
        if 592451 in current_affs:
            current_affs.remove(592451)

    # Indiana University Bloomington
    if any(inst in current_affs for inst in [4210119109]):
        if 592451 in current_affs:
            current_affs.remove(592451)

    # Inner Mongolia Agricultural University
    if any(inst in current_affs for inst in [120379545]):
        if 190774190 in current_affs:
            current_affs.remove(190774190)

    # Inner Mongolia University
    if any(inst in current_affs for inst in [2722730]):
        if 55654194 in current_affs:
            current_affs.remove(55654194)

    # Islamic Azad University Science and Research Branch
    if any(inst in current_affs for inst in [155419210]):
        if 55654194 in current_affs:
            current_affs.remove(55654194)

    # China University of Geosciences, Wuhan (need to remove 3016766249)
    if any(inst in current_affs for inst in [3124059619]):
        if 3016766249 in current_affs:
            current_affs.remove(3016766249)

    # Texas Tech University (remove 4210088475)
    if any(inst in current_affs for inst in [12315562]):
        if 4210088475 in current_affs:
            current_affs.remove(4210088475)

    # Anhui Medical University
    if any(aff_id in current_affs for aff_id in [4210136596, 4210161469, 4210149412]):
        current_affs.append(197869895)

    # Central South University (first/4210159865, second/4210153856, third/4210156904)
    if any(aff_id in current_affs for aff_id in [4210159865, 4210153856, 4210156904, 139660479]):
        current_affs.append(139660479)
        if 4210159865 in current_affs:
            if 'Second Xiangya Hospital' in aff_string:
                current_affs.remove(4210159865)
                if 4210153856 not in current_affs:
                    current_affs.append(4210153856)
            if 'Third Xiangya Hospital' in aff_string:
                if 4210159865 in current_affs:
                    current_affs.remove(4210159865)
                if 4210156904 not in current_affs:
                    current_affs.append(4210156904)

        if 4210153856 in current_affs:
            if 'Second Xiangya Hospital' not in aff_string:
                current_affs.remove(4210153856)
                if 'Third Xiangya Hospital' not in aff_string:
                    if 'Xiangya Hospital' in aff_string:
                        if 4210159865 not in current_affs:
                            current_affs.append(4210159865)

            if 'Third Xiangya Hospital' in aff_string:
                if 4210153856 in current_affs:
                    current_affs.remove(4210153856)
                if 4210156904 not in current_affs:
                    current_affs.append(4210156904)

        if 4210156904 in current_affs:
            if 'Third Xiangya Hospital' not in aff_string:
                current_affs.remove(4210156904)
                if 'Second Xiangya Hospital' not in aff_string:
                    if 'Xiangya Hospital' in aff_string:
                        if 4210159865 not in current_affs:
                            current_affs.append(4210159865)

            if 'Second Xiangya Hospital' in aff_string:
                if 4210156904 in current_affs:
                    current_affs.remove(4210156904)
                if 4210153856 not in current_affs:
                    current_affs.append(4210153856)

    # China University of Geosciences
    if any(aff_id in current_affs for aff_id in [3124059619,3125743391]):
        if 'Wuhan' in aff_string:
            if 'Beijing' not in aff_string:
                if 3125743391 in current_affs:
                    current_affs.remove(3125743391)
                    current_affs.append(3124059619)

        if 'Beijing' in aff_string:
            if 'Wuhan' not in aff_string:
                if 3124059619 in current_affs:
                    current_affs.remove(3124059619)
                    current_affs.append(3125743391)

    # Chongqing Medical University
    if any(aff_id in current_affs for aff_id in [4210159428, 4210129459, 4210097509, 4210128042, 87780372]):
        current_affs.append(87780372)
        if 4210129459 in current_affs:
            if 'Second Affiliated Hospital' in aff_string:
                current_affs.remove(4210129459)
                if 4210097509 not in current_affs:
                    current_affs.append(4210097509)

        if 4210097509 in current_affs:
            if 'First Affiliated Hospital' in aff_string:
                current_affs.remove(4210097509)
                if 4210129459 not in current_affs:
                    current_affs.append(4210129459)

    # Civil Aviation Flight University of China
    if 28813325 in current_affs:
        if 'Civil Aviation Flight University of China' in aff_string:
            current_affs.append(58995867)
            current_affs.remove(28813325)

    # Civil Aviation University of China
    if 58995867 in current_affs:
        if 'Civil Aviation University of China' in aff_string:
            current_affs.append(28813325)
            current_affs.remove(58995867)

    # Dalian Medical University
    if any(aff_id in current_affs for aff_id in [4210097509, 4210140813, 191996457]):
        current_affs.append(191996457)
        if 4210140813 in current_affs:
            if 'Second Affiliated Hospital' in aff_string:
                current_affs.remove(4210140813)
                if 4210097509 not in current_affs:
                    current_affs.append(4210097509)

        if 4210100868 in current_affs:
            if 'First Affiliated Hospital' in aff_string:
                current_affs.remove(4210100868)
                if 4210140813 not in current_affs:
                    current_affs.append(4210140813)

    # Fujian Medical University
    if any(aff_id in current_affs for aff_id in [129708740, 4210121761, 4210134617]):
        current_affs.append(129708740)
        if 4210121761 in current_affs:
            if 'Second Affiliated Hospital' in aff_string:
                current_affs.remove(4210121761)
                if 4210134617 not in current_affs:
                    current_affs.append(4210134617)

        if 4210134617 in current_affs:
            if 'First Affiliated Hospital' in aff_string:
                current_affs.remove(4210134617)
                if 4210121761 not in current_affs:
                    current_affs.append(4210121761)

    # Goethe University Frankfurt
    if 4210132578 in current_affs:
        current_affs.append(114090438)

    # Guangzhou Medical University
    if any(aff_id in current_affs for aff_id in [92039509, 4210153921, 4210105982, 4210116575, 4210098361,
                                                 4210090868, 4210092091]):
        current_affs.append(92039509)
        if 4210098361 in current_affs:
            if ('Second Affiliated Hospital' in aff_string) or ('Third Affiliated Hospital' in aff_string):
                current_affs.remove(4210098361)
                if 'Second Affiliated Hospital' in aff_string:
                    if 4210090868 not in current_affs:
                        current_affs.append(4210090868)

                if 'Third Affiliated Hospital' in aff_string:
                    if 4210092091 not in current_affs:
                        current_affs.append(4210092091)

        if 4210090868 in current_affs:
            if ('First Affiliated Hospital' in aff_string) or ('Third Affiliated Hospital' in aff_string):
                current_affs.remove(4210090868)
                if 'First Affiliated Hospital' in aff_string:
                    if 4210098361 not in current_affs:
                        current_affs.append(4210098361)

                if 'Third Affiliated Hospital' in aff_string:
                    if 4210092091 not in current_affs:
                        current_affs.append(4210092091)

        if 4210092091 in current_affs:
            if ('First Affiliated Hospital' in aff_string) or ('Second Affiliated Hospital' in aff_string):
                current_affs.remove(4210092091)
                if 'First Affiliated Hospital' in aff_string:
                    if 4210098361 not in current_affs:
                        current_affs.append(4210098361)

                if 'Second Affiliated Hospital' in aff_string:
                    if 4210090868 not in current_affs:
                        current_affs.append(4210090868)

    # Harbin Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 156144747, [4210087423],
                                                   [4210156501, 4210132813, 4210103956, 4210122309],
                                                   ['First Affiliated Hospital','Second Affiliated Hospital',
                                                    'Third Affiliated Hospital','Fourth Affiliated Hospital'])

    # Hebei Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 111381250, [4210115020],
                                                   [4210099373, 4210088328, 4210106405],
                                                   ['Second Affiliated Hospital',
                                                    'Third Affiliated Hospital','Fourth Affiliated Hospital'])

    # Jichi Medical University
    if any(aff_id in current_affs for aff_id in [4210153031, 4210166222]):
        current_affs.append(146500386)


    # Jilin University
    if any(aff_id in current_affs for aff_id in [4210125137, 4210103885]):
        current_affs.append(194450716)

    # Keimyung University
    if 4210128080 in current_affs:
        current_affs.append(52010207)

    # Kunming Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 26080491, [],
                                                   [4210120169, 2799435780, 4210165315,4210146235],
                                                   ['First Affiliated Hospital','Second Affiliated Hospital',
                                                    'Sixth Affiliated Hospital',"Yan'an Hospital"])

    # Lanzhou University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 76214153, [],
                                                   [4210163492, 4210124531],
                                                   ['First Hospital','Second Hospital'])

    # Loma Linda University
    if 1293502524 in current_affs:
        current_affs.append(26347476)

    # Louisiana State University Health Sciences Center
    if 121820613 in current_affs:
        if any(word in aff_string for word in ['Health Sciences Center','School of Medicine']):
            if 'New Orleans' in aff_string:
                current_affs.append(75420490)
            elif 'Shreveport' in aff_string:
                current_affs.append(81020160)

    # University Mohammed V
    if any(word in aff_string for word in ['Mohammed V', 'Mohammed-V']):
        if 'VI' not in aff_string:
            if any(word in aff_string for word in ['Agdal', 'Rabat', 'Morocco']):
                current_affs.append(126477371)

    # Ningxia Medical University
    if 4210139449 in current_affs:
        current_affs.append(4210127460)

    # Second Military Medical University
    if any(aff_id in current_affs for aff_id in [4210137389, 4210115928, 4210151530]):
        current_affs.append(177933477)

    # Shandong Academy of Medical Science
    if any(aff_id in current_affs for aff_id in [4210162355, 4210100830,4210156461]):
        current_affs.append(4210163399)

    # Shanxi Medical University
    if any(aff_id in current_affs for aff_id in [4210125748, 4387154184, 4210160763, 4210133678]):
        current_affs.append(17721919)

    # Tianjin Medical University
    if any(aff_id in current_affs for aff_id in [4210133270, 2800200322, 4210145773,
                                                 4210088587, 4210095816]):
        current_affs.append(5740404)

    # Wenzhou Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 27781120,
                                                   [4210156545, 4210099263, 4210158774],
                                                   [4210086973, 2801769982, 4210099512],
                                                   ['Affiliated Eye Hospital','First Affiliated Hospital',
                                                    'Second Affiliated Hospital'])

    # Xinjiang Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 154093214, [],
                                                   [2802734952, 4210165944, 4210102015, 2801820870,
                                                    4210151285],
                                                   ['First Affiliated Hospital', 'Second Affiliated Hospital',
                                                    'Third Affiliated Hospital', 'Fifth Affiliated Hospital',
                                                    'Sixth Affiliated Hospital'])

    # Xuzhou Medical College
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 177388780, [],
                                                   [4210106614, 4210140789],
                                                   ['First Affiliated Hospital', 'Second Affiliated Hospital'])

    # Nanchang University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 141649914, [],
                                                   [4210164024, 4210108480, 4210114086],
                                                   ['First Affiliated Hospital', 'Second Affiliated Hospital',
                                                    'Third Affiliated Hospital'])

    # Nantong University
    if any(aff_id in current_affs for aff_id in [4210086801, 4210085873, 4210112320, 4210122818, 4210119554]):
        current_affs.append(199305430)

    # Peking University
    if any(aff_id in current_affs for aff_id in [2802957242, 4210093964, 4210130930, 4210133846, 4210124809,
                                                 4210162420, 4210095659, 4210141942]):
        current_affs.append(20231570)

    # University of Macau
    if 111950717 in current_affs:
        if 'University of Macau' in aff_string:
            current_affs.remove(111950717)
            current_affs.append(204512498)

    # Toho University
    if any(aff_id in current_affs for aff_id in [4210125448, 4210093329, 4210095039]):
        current_affs.append(129634264)

    # Sun Yat-sen University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 157773358,
                                                   [4210128272, 4210129003, 4210119259, 4210097354,
                                                    4210146711, 4387155047],
                                                   [4210128921, 4210146956,
                                                    4210113039, 4210093460,
                                                    4210096354, 4387154481],
                                                   ['First Affiliated Hospital', 'Third Affiliated Hospital',
                                                    'Fifth Affiliated Hospital', 'Sixth Affiliated Hospital',
                                                    'Seventh Affiliated Hospital', 'Eighth Affiliated Hospital'])

    # Qingdao University
    if any(aff_id in current_affs for aff_id in [4210116869, 4210119987, 4210167271]):
        current_affs.append(108688024)

    # University of Lübeck
    if 4210112713 in current_affs:
        current_affs.append(9341345)

    # SUNY
    if 1327163397 in current_affs:
        if 'Albany' in aff_string:
            current_affs.remove(1327163397)
            current_affs.append(392282)

        elif 'Buffalo' in aff_string:
            current_affs.remove(1327163397)
            current_affs.append(63190737)

    # SUNY Upstate Medical University
    if any(aff_id in current_affs for aff_id in [4210089004, 4210114695, 4210106223]):
        current_affs.append(20388574)

    # Jiangsu University
    if any(aff_id in current_affs for aff_id in [4210111628, 4210110396, 4210124790, 4210104909,
                                                 4210139780, 4210146175]):
        current_affs.append(115592961)

    # University of Georgia
    if 4210126868 in current_affs:
        if 'Tbilisi' not in aff_string:
            current_affs.append(165733156)
            current_affs.remove(4210126868)

    # University of Eastern Piedmont Amadeo Avogadro
    if 4210119436 in current_affs:
        current_affs.append(123338534)

    # University of Colorado Denver
    if any(aff_id in current_affs for aff_id in [4210096275, 2801983979, 3019586173, 2802547023,
                                                 4210134151, 1288162130]):
        current_affs.append(921990950)

    # University of Tennessee at Knoxville
    if 2802076678 in current_affs:
        current_affs.append(75027704)

    # Xuzhou Medical College
    if any(aff_id in current_affs for aff_id in [4210106614, 4210140789, 4210124997, 4210143072]):
        current_affs.append(177388780)

    # UT Southwestern Medical Center
    if any(aff_id in current_affs for aff_id in [4210096815]):
        current_affs.append(867280407)

    # University of Reims Champagne-Ardenne
    if 4210105796 in current_affs:
        current_affs.append(96226040)

    # Soochow University
    if any(aff_id in current_affs for aff_id in [4210151382, 4210153519, 4210166543, 4210123502, 4210124971,
                                                 4210133251, 4210108364]):
        current_affs.append(3923682)

    # Virginia Tech
    if 4210166782 in current_affs:
        current_affs.append(859038795)
        current_affs.remove(4210166782)

    # University Canada West
    if 214977574 in current_affs:
        if 'University Canada West' not in aff_string:
            current_affs.remove(214977574)

    # Universidad Central de Chile
    if 4210156023 in current_affs:
        if any(word in aff_string for word in ['Universidad Central de Chile','UCEN', 'Central University of Chile']):
            pass
        elif any(word in aff_string for word in ['Universidad de Chile']):
            current_affs.remove(4210156023)
            current_affs.append(69737025)
        elif any(word in aff_string for word in ['Pontificia Universidad Católica de Chile']):
            current_affs.remove(4210156023)
            current_affs.append(162148367)
        else:
            current_affs.remove(4210156023)

    # Hamilton College (getting confused with Hamilton Institute at Maynooth University)
    if 188592606 in current_affs:
        if 'Hamilton Institute' in aff_string:
            current_affs.remove(188592606)

    # University of Quebec
    if any(inst in current_affs for inst in [159129438,63341726,104914703,182451676,33217400,190270569,
                                              39481719,31571312,9736820,200745827]):
        if 49663120 in current_affs:
            current_affs.remove(49663120)

    u_quebec_strings = ['Université du Québec','University of Quebec','University of Québec','Quebec University',
                             'Universite du Quebec','Univ Quebec','Univ. Quebec','Univ Québec','Univ. Québec',
                             'Quebec Univ.','Québec Univ.','Univ. of Quebec','Univ. of Québec','U de Québec',
                             'U de Quebec','Univ. du Quebec','Univ. du Québec']

    # Removing bad University of Quebec matches from Laval U. and U. of Montreal
    if 49663120 in current_affs:
        if any(word in aff_string for word in ['Laval University','University of Montreal']):
            if any(word in aff_string for word in u_quebec_strings):
                pass
            else:
                current_affs.remove(49663120)
                if 'Laval University' in aff_string:
                    current_affs.append(43406934)
                elif 'University of Montreal' in aff_string:
                    current_affs.append(70931966)

    # University of Maryland
    if 116545467 in current_affs:
        if 'University of Maryland' in aff_string:
            current_affs.remove(116545467)
            if 'Baltimore' in aff_string:
                if 'Baltimore County' in aff_string:
                    current_affs.append(79272384)
                else:
                    current_affs.append(126744593)
            elif any(word in aff_string for word in ['Princess Anne','Eastern Shore']):
                current_affs.append(22407884)
            elif 'College Park' in aff_string:
                current_affs.append(66946132)
    if any(aff_id in current_affs for aff_id in [4210132871,1315496137]):
        current_affs.append(126744593)

    # Comenius University Bratislava
    if 4210095125 in current_affs:
        current_affs.append(74788687)

    # University of North Texas
    if 165139151 in current_affs:
        current_affs.append(123534392)

    # University of Veterinary Medicine Hannover, Foundation
    if 114112103 in current_affs:
        if 'University of Veterinary Medicine' in aff_string:
            current_affs.append(189991)
            current_affs.remove(114112103)

    # University of South China
    if 4210143187 in current_affs:
        current_affs.append(91935597)

    # Wuhan University
    if any(aff_id in current_affs for aff_id in [4210131162, 4210125402, 4210140357,
                                                 4210126156, 4210120234]):
        current_affs.append(37461747)

    # SUNY Upstate Medical University
    if any(aff_id in current_affs for aff_id in [4210089004, 4210114695, 4210106223]):
        current_affs.append(20388574)

    # University College London
    if 2800173700 in current_affs:
        if 'University College London' in aff_string:
            current_affs.remove(2800173700)
        elif 'UCL' in aff_string:
            if 'London' in aff_string:
                current_affs.remove(2800173700)

    if any(aff_id in current_affs for aff_id in [2800129641, 4210150574, 4210141040, 4210151618,
                                                 2802751111, 4210118734, 2801138448, 1289784979,
                                                 4210119772, 2802844630, 2802576581]):
        current_affs.append(45129253)

    # Radboud University
    if any(aff_id in current_affs for aff_id in [4210109357, 2802934949]):
        current_affs.append(145872427)
        current_affs.append(2802934949)
    elif any(aff_id in current_affs for aff_id in [2801238018, 4210126394]):
        current_affs.append(145872427)

    # Shanghai University of Traditional Chinese Medicine
    if any(aff_id in current_affs for aff_id in [4210101984, 4210149132, 4210124018, 4210133418]):
        current_affs.append(4210098460)

    # Technical University Dortmund
    if 4210166399 in current_affs:
        current_affs.append(200332995)

    # University of Liverpool
    if any(aff_id in current_affs for aff_id in [2802775644, 4210089066, 4210165020, 2799272705, 4210157731,
                                                 2801018919, 4210134784, 4210086861, 4210093008, 4210157668,
                                                 2799451200, 4210111181, 2802833755, 4210086643]):
        current_affs.append(146655781)

    # Sichuan University
    if any(aff_id in current_affs for aff_id in [4210089228, 4210089761]):
        current_affs.append(24185976)

    # Johns Hopkins University
    if any(aff_id in current_affs for aff_id in [4210098865, 2802697821, 4210129832, 2799853436]):
        current_affs.append(145311948)

    # Shantou University
    if any(aff_id in current_affs for aff_id in [4210091098, 4210115239, 4210120522, 4210121200, 4210109945]):
        current_affs.append(32574673)

    # Johannes Gutenberg University Mainz
    if any(aff_id in current_affs for aff_id in [4210094062, 4387156336, 4210148626]):
        current_affs.append(197323543)

    # Yokohama City University
    if 2802180866 in current_affs:
        current_affs.append(89630735)
    elif 4210099218 in current_affs:
        current_affs.append(89630735)

    # Tampere University
    if 150589677 in current_affs:
        if 'Tampere University of Technology' in aff_string:
            if 'Tampere University of Applied Sciences' not in aff_string:
                current_affs.append(4210133110)
                current_affs.remove(150589677)

    # University of Ulster
    if any(aff_id in current_affs for aff_id in [2802259370,2802808109,4210130048]):
        current_affs.append(138801177)

    # Bloomberg
    if 1299907687 in current_affs:
        if 'Bloomberg School of' in aff_string:
            current_affs.remove(1299907687)
            current_affs.append(145311948)

    # Coventry (UK)
    if 4210127762 in current_affs:
        if 39555362 in current_affs:
            current_affs.remove(4210127762)

    # Hershey
    if 123457487 in current_affs:
        if any(aff_id in current_affs for aff_id in [130769515, 82783531]):
            current_affs.remove(123457487)

    # Södra Skogsägarna
    if 4210151240 in current_affs:
        if not any(word in aff_string for word in ['Södra Skogsägarna','Sodra Skogsägarna','Sodra Skogsagarna']):
            current_affs.remove(4210151240)

    # Bayer
    if 67348948 in current_affs:
        if 'Bayerisch' in aff_string:
            if 'Germany' in aff_string:
                current_affs.remove(67348948)

    # Eppendorf
    if 2801537753 in current_affs:
        if any(aff_id in current_affs for aff_id in [159176309, 4210108711, 4210125929]):
            current_affs.remove(2801537753)
        elif any(word in aff_string for word in ['University Medical Center',
                                                 'University Medical Centre']):
            if 'Hamburg' in aff_string:
                current_affs.remove(2801537753)
                current_affs.append(4210108711)
                current_affs.append(159176309)
        elif 'Hamburg-Eppendorf' in aff_string:
            current_affs.append(4210108711)
            current_affs.append(159176309)


    # Applied Mathematics
    if 4210131439 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Applied Mathematics Inst',
                                               'Applied Mathematics Dep',
                                               'Applied Mathematics Div']):
            current_affs.remove(4210131439)

    # Applied Materials (Germany)
    if 4210165146 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Applied Materials Inst',
                                               'Applied Materials Dep',
                                               'Applied Materials Div']):
            current_affs.remove(4210165146)
        elif len(current_affs) > 1:
            current_affs.remove(4210165146)

    # Applied Materials (United States)
    if 193427800 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Applied Materials Inst',
                                               'Applied Materials Dep',
                                               'Applied Materials Div']):
            current_affs.remove(193427800)
        elif len(current_affs) > 1:
            current_affs.remove(193427800)

    # Applied Materials (Israel)
    if 4210100008 in current_affs:
        if len(current_affs) > 1:
            current_affs.remove(4210100008)

    # Applied Materials (U.K.)
    if 4210087370 in current_affs:
        if len(current_affs) > 1:
            current_affs.remove(4210087370)

    # Applied Materials (Singapore)
    if 4210102879 in current_affs:
        if len(current_affs) > 1:
            current_affs.remove(4210102879)

    # Bioengineering (Switzerland)
    if 4210092485 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Bioengineering Inst',
                                               'Bioengineering Dep',
                                               'Bioengineering Div']):
            current_affs.remove(4210092485)

    # Visual Sciences (USA)
    if 4210107648 in current_affs:
        if 'Raleigh' not in aff_string:
            current_affs.remove(4210107648)

    # Quantum Group (USA)
    if 4210090401 in current_affs:
        if 'San Diego' not in aff_string:
            current_affs.remove(4210090401)

    # Engineering (Italy)
    if 4210127672 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Engineering Inst',
                                               'Engineering Dep',
                                               'Engineering Div']):
            current_affs.remove(4210127672)

    # Neurobehavioral Research (USA)
    if 4210157590 in current_affs:
        if 'Neurobehavioral Research' not in aff_string:
            current_affs.remove(4210157590)
        elif any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for']):
            current_affs.remove(4210157590)

    # Materials Research Institute (USA)
    if 4210148571 in current_affs:
        if 'Dayton' not in aff_string:
            current_affs.remove(4210148571)


    # Materials Sciences (USA)
    if 4210111788 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Engineering Inst',
                                               'Engineering Dep',
                                               'Engineering Div']):
            current_affs.remove(4210111788)
        elif 'Horsham' not in aff_string:
            current_affs.remove(4210111788)

    # Surgical Science (Sweden)
    if 4210147392 in current_affs:
        if 'Göteborg' not in aff_string:
            current_affs.remove(4210147392)

    # Computational Sciences (USA)
    if 4210129810 in current_affs:
        if 'Madison' not in aff_string:
            current_affs.remove(4210129810)

    # Translational Sciences (USA)
    if 4210163070 in current_affs:
        if 'Memphis' not in aff_string:
            current_affs.remove(4210163070)

    # Institut Henri Poincaré
    if ('henri' in aff_string.lower()) | (51178685 in current_affs):
        if any(word in aff_string.lower() for word in ['henri poincaré','henri poincare']) | (51178685 in current_affs):
            if 51178685 in current_affs:
                if 'IHP Group' in aff_string:
                    current_affs.remove(51178685)
                elif re.search('\\bIHP\\b', aff_string) and ('Paris' in aff_string):
                    pass
                elif not any(word in aff_string for word in ['Institut Henri Poincaré','Henri Poincaré Institut','Institut Henri Poincaré',
                                                        'Henri Poincaré Institut','Institute Henri Poincare']):
                    current_affs.remove(51178685)

                if 51178685 not in current_affs:
                    if any(word in aff_string for word in ['Henri Poincaré','Henri Poincare']) and ('Nancy' in aff_string):
                        current_affs.append(90183372)

            if any(word in aff_string for word in ['Henri Poincaré','Henri Poincare']):
                if 'Nancy' in aff_string:
                    current_affs.append(90183372)
                elif any(word in aff_string for word in ['Institut Henri Poincaré','Henri Poincaré Institut','Institut Henri Poincaré',
                                                        'Henri Poincaré Institut','Institute Henri Poincare']):
                    current_affs.append(51178685)

    # Roskilde University
    if 'Roskilde' in aff_string:
        if any(word in aff_string.lower() for
               word in ['Roskilde Uni','University of Roskilde', 'Universidade de Roskilde',
                        'Rosikilde University','Universities of Roskilde and Copenhagen']) & (107707843 in current_affs):
            current_affs.remove(107707843)

    # Hochschule Hannover
    if 140025399 in current_affs:
        if any(word in aff_string.lower() for word in ['medizinische','medical','med.','medische']):
            current_affs.remove(140025399)
            current_affs.append(34809795)
        elif any(word in aff_string.lower() for word in ['technische','techn.']):
            current_affs.remove(140025399)
            current_affs.append(114112103)

    # Twitter
    if 113979032 in current_affs:
        if '@' in aff_string:
            current_affs.remove(113979032)

    # BIOM
    if 4210131549 in current_affs:
        if any(word in aff_string for word in ["BOME", "Biologie des organismes marins et écosystèmes"]):
            current_affs.remove(4210131549)
            current_affs.append(4210110009)
        elif any(word in aff_string for word in ["ISOMER","Institut Des Substances et Organismes de la Mer"]):
            current_affs.remove(4210131549)
            current_affs.append(4210144488)
        elif any(word in aff_string for word in ["BOREA", "Biologie des Organismes et Ecosystèmes Aquatiques"]):
            current_affs.remove(4210131549)
            current_affs.append(4210110009)
        elif any(word in aff_string for word in ["EFNO", "Ecosystèmes forestiers","BioMEA","PFOM",
                                                 "Physiologie Fonctionnelle des Organismes Marins"]):
            current_affs.remove(4210131549)

    # Gateway
    if 4210139101 in current_affs:
        current_affs.remove(4210139101)

    # Australian College of Theology
    if 2800615496 in current_affs:
        if 'theology' not in aff_string.lower():
            current_affs.remove(2800615496)

    # Southern Institute of Technology
    if 2802042008 in current_affs:
        if not any(word in aff_string for word in ['Southern Institute of Technology','SIT']):
            current_affs.remove(2802042008)

    # Access e.V.
    if 4210121009 in current_affs:
        if not any(word in aff_string.lower() for word in ['access e.v.', 'access e. v.', 'accessmm e. v.', 'access ev ',
                                                'access ev.','access ev,']):
            current_affs.remove(4210121009)

    # Manipal University Jaipur
    if 164861460 in current_affs:
        if 'manipal university jaipur' in aff_string.lower():
            current_affs.remove(164861460)
            current_affs.append(73779912)

    # Westfälische Hochschule
    if 4210145899 in current_affs:
        if 887968799 in current_affs:
            current_affs.remove(4210145899)

    # École des Ponts ParisTech
    if 142631665 in current_affs:
        if any(word in aff_string.lower() for word in ['laboratoire central des ponts',
                                                       'laboratoire régional des ponts',
                                                       'laboratoire regional des ponts']) | ('LCPC' in aff_string):
            if not any(word in aff_string.lower() for word in ['ecole nationale des ponts','ecole des ponts paristech']):
                current_affs.remove(142631665)


    if not current_affs:
        current_affs.append(-1)

    return list(set(current_affs))
