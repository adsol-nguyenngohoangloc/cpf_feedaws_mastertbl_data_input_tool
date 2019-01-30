#!/bin/csh

# Postgresqlに接続するための情報を定義
set PGHOST = "con-res-es-dbcluster-feed.cluster-ctewkwdmcpdj.ap-northeast-1.rds.amazonaws.com"
set PGPORT = "5432"
set PGUSERNAME = "feedmaster"
setenv PGPASSWORD "feedpassword"
set PGDBNAME = "con_res_es_db_feed"

if ($#argv != 1) then
    echo "経路名を指定してください。"
    exit 1
endif

set jobnetid = $1

# 記事（蓄積）マスタデータテーブルのデータ更新用SQLファイル
set master_table_name = "article_archive_master"

# 記事（蓄積）更新対象管理テーブルのデータ更新用SQLファイル
set updmng_table_name = "article_archive_update_manage"

set delete_master_table_hina = "delete_master_table.hina"
set delete_master_table_sql  = "delete_master_table.sql"
set updatetime_list = "updatetime.list"
set master_table_column = '(articleid,ipcode,mediag1,mediag1seq,mediag2,mediag2seq,mediag3,mediag3seq,mediacode,mediacodeseq,media,date,date2,sendtime,jobnetid,updatetime,updatetime2,addtime,addtime2,deleteflag,men,pagefrom,pageto,pagefrom2,pageto2,picture,paragraph,length,chargeflg,copyflg,copycorp,credit,genrel,genrem,genres,headline,bodyflag,pagecount,author,refferid,body,htmlsource,tableflag,atrflag,kind,kind2,impflag,keyword,tkeyword,tlaei_keywords_g_category,tlaei_keywords_g_tncode,tlaei_keywords_g_ttcode,tlaei_keywords_g_oversea_company_num,tlaei_keywords_g_corp_num,tlaei_keywords_g_needs_ind_code_l,tlaei_keywords_g_needs_ind_code_m,tlaei_keywords_g_needs_ind_code_s,tlaei_keywords_g_master,tlaei_keywords_g_read,tlaei_keywords_g_scope,tlaei_keywords_g_score,tlaei_keywords_g_notation,tlaei_keywords_g_subject,bunrui,tbunrui,"column",classification,category,rnationalcd,rrgncd,dnationcd,drgncd,tlaei_company_g_tcpjname,tlaei_company_g_ttcode,tlaei_company_g_tncode,tlaei_company_g_tcpattribute1,tlaei_company_g_tcpattribute2,tlaei_company_g_tcpattribute3,tlaei_company_g_corp_num,tlaei_company_g_needs_ind_code_l,tlaei_company_g_needs_ind_code_m,tlaei_company_g_needs_ind_code_s,tlaei_company_all_g_tcpjnameall,tlaei_company_all_g_ttcodeall,tlaei_company_all_g_tncodeall,tlaei_company_all_g_corp_num,tlaei_company_all_g_needs_ind_code_l,tlaei_company_all_g_needs_ind_code_m,tlaei_company_all_g_needs_ind_code_s,tlaei_company_rc_g_tcpjname,tlaei_company_rc_g_ttcode,tlaei_company_rc_g_tncode,tlaei_company_rc_g_oversea_company_code,tlaei_company_rc_g_address,tlaei_company_rc_g_manager,tlaei_company_rc_g_score,tlaei_company_rc_all_g_tcpjnameall,tlaei_company_rc_all_g_ttcodeall,tlaei_company_rc_all_g_tncodeall,tlaei_company_rc_all_g_oversea_company_code,tlaei_company_rc_all_g_address,tlaei_company_rc_all_g_manager,tlaei_company_rc_all_g_score,togname,tognameall,tpersonm,tperson,tpersonall,tlaei_person_rc_g_name,tlaei_person_rc_g_birth_year,tlaei_person_rc_g_score,tlaei_person_rc_all_g_name,tlaei_person_rc_all_g_birth_year,tlaei_person_rc_all_g_score,tplace,tplaceall,tother,totherall,tlaei_theme_g_ttopiclcode,tlaei_theme_g_ttopicmcode,tlaei_theme_g_ttopicscode,tlaei_theme_g_ttopiclname,tlaei_theme_g_ttopicmname,tlaei_theme_g_ttopicsname,tlaei_theme_rc_g_ttopiclcode,tlaei_theme_rc_g_ttopicmcode,tlaei_theme_rc_g_ttopicscode,tlaei_theme_rc_g_ttopiclname,tlaei_theme_rc_g_ttopicmname,tlaei_theme_rc_g_ttopicsname,tlaei_theme_rc_g_score,tlaei_article_g_tkindcode,tlaei_article_g_tkindname,tlaei_region_g_tareacode,tlaei_region_g_tareaname,tlaei_region_g_region_genre,tlaei_region_g_region_genre_name,tlaei_ind_g_tindstlcode,tlaei_ind_g_tindstmcode,tlaei_ind_g_tindstscode,tlaei_ind_g_tindstlname,tlaei_ind_g_tindstmname,tlaei_ind_g_tindstsname,tcomyobi1,tcomyobi2,tcomyobi3,tcomyobi4,tcomyobi5,tspyobi1,tspyobi2,tspyobi3,tspyobi4,tspyobi5,company_g_ntcode,company_g_tcode,company_g_cpjnam,company_g_cpjmarketnam,cpjnam2,cpenam,cpurl,telecom_ind_g_indstcode,telecom_ind_g_indstnam,attach_file_g_attname,attach_file_g_attgroup,attach_file_g_attsize,attach_file_g_attcaption,attach_file_g_attftype,attach_file_g_attpdfnum,attach_file_g_attthumbjpg,attach_file_g_attthumtype,attach_file_g_attphotodate,attach_file_g_attcountry,attach_file_g_attprefec,attach_file_g_attsend,attach_file_g_attcameraman,attach_file_g_attcommonst1,attach_file_g_attcommonst2,attach_file_g_attcommonst3,attach_file_g_attspecialst1,attach_file_g_attspecialst2,attach_file_g_attspecialst3,commonst1,commonst2,commonst3,commonst4,commonst5,commonst6,commonst7,commonst8,commonst9,commonst10,commonst11,commonst12,commonst13,commonst14,commonst15,commonnum1,commonnum2,commonnum3,commonnum4,commonnum5,commonnum6,commonnum7,commonnum8,commonnum9,commonnum10,commonnum11,commonnum12,specialst1,specialst2,specialst3,specialst4,specialst5,specialst6,specialst7,specialst8,specialst9,specialst10,specialst11,specialst12,specialnum1,specialnum2,specialnum3,specialnum4,specialnum5,specialnum6,specialnum7,specialnum8,permission_level,permission_term)'
#------------------------------------------------------------------------
# データ更新開始
#------------------------------------------------------------------------
# 入力CSVファイルを格納するDIR
set INPUT_FILE_DIR = TESTDATA/CSV_OUTPUT
# 入力の削除電文JSONファイルを格納するDIR
set INPUT_DELFILE_DIR = TESTDATA/DEL_FILE

# データディレクトリを削除して、再作成
rm -rf ${INPUT_FILE_DIR}
rm -rf ${INPUT_DELFILE_DIR}
mkdir ${INPUT_FILE_DIR}
mkdir ${INPUT_DELFILE_DIR}

# 削除本文JSONをTESTDATA/DEL_FILEに移動
mv TESDATA/.*DELETE*.json ${INPUT_DELFILE_DIR}

# 更新用JSONからCSVを作成し、TESTDATA/CSV_OUTPUTに移動\
java -jar makecsvfromjson.jar TESTDATA

# 入力CSVファイルのリストを作成
ls -1 ${INPUT_FILE_DIR} | egrep "*.csv" > input_file_list
# 削除電文のJSONファイルリストをリスト追加
ls -1 ${INPUT_DELFILE_DIR} >> input_file_list

# 入力CSVファイル数を取得
@ num_of_input_file = `wc -l input_file_list | awk '{print $1}'`

if ( ${num_of_input_file} == 0 ) then
    echo "処理対象のCSVファイルが存在しません。処理を終了します。"
    exit 0
endif

@ i = 0

while( ${i} < ${num_of_input_file} )
    set update_manage_table_sql  = "INSERT INTO article_archive_update_manage VALUES "
    # 現在処理しているファイルの位置を取得
    @ now_process_file_num = ${i} % ${num_of_input_file} + 1
    set input_file_name = "`cat input_file_list | head -${now_process_file_num} | tail -1`"
    echo "${input_file_name}のデータをマスタデータに登録開始!"

    # 削除電文の場合
    echo "${input_file_name}" | fgrep "DELETE" > /dev/null
    if( $? == 0 ) then
        cp ${delete_master_table_hina} ${delete_master_table_sql}_tmp

        # updatetime情報を取得し、update文に更新
        set updatetime = `date -d '9 hour' "+%Y%m%d%H%M%S"`
        sed -e "s/updatetime_value/${updatetime}/" ${delete_master_table_sql}_tmp > ${delete_master_table_sql}_tmp1

        # jobnetid情報を取得し、update文に更新
        #set jobnetid = `echo "${input_file_name}" | cut -c1-3`
        sed -e "s/jobnetid_value/${jobnetid}/" ${delete_master_table_sql}_tmp1 > ${delete_master_table_sql}

        # 入力ファイルの行数を取得
        @ line_of_input_file = `wc -l ${INPUT_DELFILE_DIR}/${input_file_name} | awk '{print $1}'`
        @ j = 1
        while ( ${j} <= ${line_of_input_file} )
            set line = "`cat ${INPUT_DELFILE_DIR}/${input_file_name} | head -${j} | tail -1`"
            set articleid = `echo "${line}" | cut -d ':' -f 5 | cut -d '"' -f 2`
            echo "'${articleid}'" >> ${delete_master_table_sql}
            if( ${j} < ${line_of_input_file} ) then
                echo "," >> ${delete_master_table_sql}
            else
                echo ");" >> ${delete_master_table_sql}
            endif

            @ j++
        end

        # 記事（蓄積）マスタデータテーブルのデータを論理削除するSQL実行
        psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSERNAME} -d ${PGDBNAME} -f ${delete_master_table_sql}

        set master_updatetime = `date -d '9 hour' "+%Y%m%d%H%M%S"`
        # 更新対象管理テーブル更新用SQLを作成
        set update_manage_table_sql = "${update_manage_table_sql}('${jobnetid}', ${updatetime}, ${master_updatetime});"
        # 更新対象管理テーブル更新用SQLを実行
        psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSERNAME} -d ${PGDBNAME} -c "${update_manage_table_sql}"

    # 登録・更新電文の場合
    else
        set index_master_sql = "\COPY ${master_table_name} ${master_table_column} FROM '${INPUT_FILE_DIR}/${input_file_name}' WITH CSV HEADER QUOTE '"'"
        # 記事（蓄積）マスタデータテーブルを更新するSQL実行
        psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSERNAME} -d ${PGDBNAME} -c "${index_master_sql}"

        # 更新対処管理テーブル更新を更新するためのSQLを作成
        cat ${INPUT_FILE_DIR}/${input_file_name} | cut -d ',' -f 16 | cut -d '"' -f 2 | sort | uniq > ${updatetime_list}
        @ num_of_updatetime = `wc -l ${updatetime_list} | awk '{print $1}'`
        @ num_of_updatetime = ${num_of_updatetime} - 1
        @ j = 1
        while ( ${j} <= ${num_of_updatetime} )
            set line = "`cat ${updatetime_list} | head -${j} | tail -1`"
            set updatetime = `echo "${line}"`
            set master_updatetime = `date -d '9 hour' "+%Y%m%d%H%M%S"`
            set update_manage_table_sql = "${update_manage_table_sql}('${jobnetid}', ${updatetime}, ${master_updatetime})"
            if( ${j} == ${num_of_updatetime} ) then
                set update_manage_table_sql = "${update_manage_table_sql} ON CONFLICT (jobnetid, updatetime, master_updatetime) DO NOTHING;"
            else
                set update_manage_table_sql = "${update_manage_table_sql},"
            endif

            @ j++
        end
        psql -h ${PGHOST} -p ${PGPORT} -U ${PGUSERNAME} -d ${PGDBNAME} -c "${update_manage_table_sql}"
    endif

    @ i++
    echo "${input_file_name}ののデータをマスタデータに登録終了!"
    @ rand = `date "+%Y%m%d%H%M%S"`
    @ rand = ${rand} % 2 + 1
    echo "sleep ${rand} minute(s)"
    sleep ${rand}m
end