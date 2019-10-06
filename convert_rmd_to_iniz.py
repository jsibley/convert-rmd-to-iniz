#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description:
    A tool to convert the payload from the OpenMRS Reference Metadata (RMD)
    module into objects that can be processed by the Initializer (Iniz) module.

Steps:
    1. Extract Metadata Sharing (MDS) packages
    2. Convert XML-defined concept metatdata into an MDS package
    3. Convert XML-defined concepts into CSV

Usage:
    ./rmd_to_iniz.py <path to Reference Metadata OMOD file> <path to write
    'configuration' output directory>
"""

import csv, datetime, getopt, os, re, sys, uuid, zipfile
import xml.etree.cElementTree as et

def build_concept_csv(concept_csv, concepts, descriptions, answers, names,
                      ref_maps, sets, ref_dicts):
    # Process all concept objects converting from XML to CSV
    header_lookup = dict([("allow_decimal", "Allow decimals"),
                          ("display_precision", "Display precision"),
                          ("hi_absolute", "Absolute high"),
                          ("hi_critical", "Critical high"),
                          ("hi_normal", "Normal high"),
                          ("low_absolute", "Absolute low"),
                          ("low_critical", "Critical low"),
                          ("low_normal", "Normal low"),
                          ("precise", "Allow decimals"),
                          ("retired", "Void/Retire"),
                          ("units", "Units"),
                          ("uuid", "Uuid")])
    
    for key in concepts.keys():
        concept_csv[key] = dict()
        for attrib_key in concepts[key].keys():
            if attrib_key in ["retired", "uuid", "hi_absolute", "units"
                              "hi_critical", "hi_normal", "low_absolute",
                              "low_critical", "low_normal", "allow_decimal",
                              "precise", "display_precision"] :
                concept_csv[key][header_lookup[attrib_key]] = \
                concepts[key][attrib_key]
            elif attrib_key == "datatype_id":
                concept_csv[key]["Data type"] = \
                ref_dicts["Datatype"][concepts[key][attrib_key]]
            elif attrib_key == "class_id":
                concept_csv[key]["Data class"] = \
                ref_dicts["Class"][concepts[key][attrib_key]]
            else:
                continue

    # Process all description objects converting from XML to CSV
    for key in descriptions.keys():
        concept_csv[descriptions[key]["concept_id"]]["Description:" + 
                   descriptions[key]["locale"]] = \
                   descriptions[key]["description"]

    # Process all answer objects converting from XML to CSV
    for key in answers.keys():
        if not "Answers" in concept_csv[answers[key]["concept_id"]].keys():
            concept_csv[answers[key]["concept_id"]]["Answers"] = ""
        concept_csv[answers[key]["concept_id"]]["Answers"] += \
            concepts[answers[key]["answer_concept"]]["uuid"] + ";"
    for key in answers.keys():
        concept_csv[answers[key]["concept_id"]]["Answers"] = \
        concept_csv[answers[key]["concept_id"]]["Answers"].rstrip(";")

    # Process all name objects converting from XML to CSV
    for key in names.keys():
        if not "concept_name_type" in names[key].keys():
            concept_csv[names[key]["concept_id"]]["Fully specified name:" + \
                       names[key]["locale"]] = names[key]["name"]
        else:
            if names[key]["concept_name_type"] == "SHORT":
                concept_csv[names[key]["concept_id"]]["Short name:" + \
                           names[key]["locale"]] = names[key]["name"]
            else:
                concept_csv[names[key]["concept_id"]]["Fully specified name:" \
                           + names[key]["locale"]] = names[key]["name"]

    # Process all reference map objects converting from XML to CSV
    for key in ref_maps.keys():
        if not "Same as mappings" in concept_csv[ref_maps[key]\
                                                 ["concept_id"]].keys():
            concept_csv[ref_maps[key]["concept_id"]]["Same as mappings"] = ""
        concept_csv[ref_maps[key]["concept_id"]]["Same as mappings"] += \
        ref_dicts["ReferenceTerm"][ref_maps[key]["concept_reference_term_id"]]\
        + ";"
    for key in ref_maps.keys():
        concept_csv[ref_maps[key]["concept_id"]]["Same as mappings"] = \
        concept_csv[ref_maps[key]["concept_id"]]["Same as mappings"].\
        rstrip(";")

    # Process all set objects converting from XML to CSV
    for key in sets.keys():
        if not "Members" in concept_csv[concepts[sets[key]\
                            ["concept_set"]]\
                            ["concept_id"]].keys():
            concept_csv[concepts[sets[key]["concept_set"]]\
                        ["concept_id"]]["Members"] = ""
        concept_csv[concepts[sets[key]["concept_set"]]["concept_id"]]\
        ["Members"] += concepts[sets[key]["concept_id"]]["uuid"] + ";"
    for key in sets.keys():
        concept_csv[concepts[sets[key]["concept_set"]]["concept_id"]]\
        ["Members"] =\
        concept_csv[concepts[sets[key]["concept_set"]]["concept_id"]]\
        ["Members"].rstrip(";")

def build_concept_metadata_mds_header_xml(name, desc, version, datatypes,
                                          classes, map_types, ref_sources,
                                          ref_terms):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    id_cnt = 1
    module_list = [("calculation", "1.2"), ("legacyui", "1.5.0"),
                   ("referenceapplication", "2.9.0"), ("attachments", "2.1.0"),
                   ("metadatadeploy", "1.11.0"), ("fhir", "1.18.0"),
                   ("appointmentscheduling", "1.10.0"), ("atlas", "2.2"),
                   ("metadatasharing", "1.5.0"),
                   ("appointmentschedulingui", "1.7.0"),
                   ("allergyui", "1.8.1"), ("formentryapp", "1.4.2"),
                   ("chartsearch", "2.1.0"), ("providermanagement", "2.10.0"),
                   ("htmlwidgets", "1.10.0"), ("dataexchange", "1.3.3"),
                   ("referencedemodata", "1.4.4"),
                   ("addresshierarchy", "2.11.0"), ("reportingrest", "1.10.0"),
                   ("uiframework", "3.15.0"), ("reportingui", "1.6.0"),
                   ("referencemetadata", "2.9.0"), ("event", "2.7.0"),
                   ("uicommons", "2.6.1"), ("reporting", "1.17.0"),
                   ("emrapi", "1.27.0"), ("registrationapp", "1.13.0"),
                   ("idgen", "4.5.0"), ("htmlformentryui", "1.7.0"),
                   ("htmlformentry", "3.8.0"), ("appframework", "2.13.0"),
                   ("reportingcompatibility", "2.0.6"), ("uilibrary", "2.0.6"),
                   ("webservices.rest", "2.24.0.573609"), ("adminui", "1.2.4"),
                   ("registrationcore", "1.8.0"), ("owa", "1.10.0"),
                   ("metadatamapping", "1.3.4"), ("coreapps", "1.21.0"),
                   ("appui", "1.9.0"), ("serialization.xstream", "0.2.14")]
    
    package_el = et.Element("package")
    package_el.set("id", str(id_cnt))
    id_cnt += 1
    package_el.set("uuid", str(uuid.uuid4()))
    dateCreated_el = et.SubElement(package_el, "dateCreated")
    dateCreated_el.set("id", str(id_cnt))
    id_cnt += 1
    dateCreated_el.text = now
    name_el = et.SubElement(package_el, "name")
    name_el.text = name
    description_el = et.SubElement(package_el, "description")
    description_el.text = desc
    openmrsVersion_el = et.SubElement(package_el, "openmrsVersion")
    openmrsVersion_el.text = "2.1.4  Build 2cb839"
    version_el = et.SubElement(package_el, "version")
    version_el.text = version
    groupUuid_el = et.SubElement(package_el, "groupUuid")
    groupUuid_el.text = str(uuid.uuid4())
    modules_el = et.SubElement(package_el, "modules")
    modules_el.set("id", str(id_cnt))
    id_cnt += 1
    for module in module_list:
        entry_el = et.SubElement(modules_el, "entry")
        string_el = et.SubElement(entry_el, "string")
        string_el.text = module[0]
        string_el = et.SubElement(entry_el, "string")
        string_el.text = module[1]
    items_el = et.SubElement(package_el, "items")
    items_el.set("class", "linked-hash-set")
    items_el.set("id", str(id_cnt))
    id_cnt += 1
    item_metadata = \
        [(datatypes, "concept_datatype_id", "ConceptDatatype", 0),
         (classes, "concept_class_id", "ConceptClass", 0),
         (map_types, "concept_map_type_id", "ConceptMapType", 1),
         (ref_sources, "concept_source_id", "ConceptSource", 0),
         (ref_terms, "concept_reference_term_id", "ConceptReferenceTerm", 1)]
    for item_list in item_metadata:
        id_cnt = build_concept_metadata_mds_header_xml_items(items_el, id_cnt,
                                                             item_list)
    relatedItems_el = et.SubElement(package_el, "relatedItems")
    relatedItems_el.set("class", "linked-hash-set")
    relatedItems_el.set("id", str(id_cnt))
    
    return package_el

def build_concept_metadata_mds_header_xml_items(items_el, id_cnt, item_list):
    date_changed_field = "date_changed"
        
    for key in item_list[0]:
        if item_list[3] == 0 or \
            date_changed_field not in item_list[0][key].keys():
            date_changed_field = "date_created"
        
        item_el = et.SubElement(items_el,
                                "org.openmrs.module.metadatasharing.Item")
        item_el.set("id", str(id_cnt))
        id_cnt += 1
        item_el.set("uuid", key)
        id_el = et.SubElement(item_el, "id")
        id_el.text = item_list[0][key][item_list[1]]
        classname_el = et.SubElement(item_el, "classname")
        classname_el.text = "org.openmrs." + item_list[2]
        dateChanged_el = et.SubElement(item_el, "dateChanged")
        dateChanged_el.set("id", str(id_cnt))
        id_cnt += 1
        dateChanged_el.text = item_list[0][key][date_changed_field]
        retired_el = et.SubElement(item_el, "retired")
        if "retired" in item_list[0][key].keys():
            retired_el.text = item_list[0][key]["retired"]
        
    return id_cnt

def build_concept_metadata_mds_metadata_xml(datatypes, classes, map_types,
                                            ref_sources, ref_terms):
    id_cnt = 1
    list_el = et.Element("list")
    list_el.set("id", str(id_cnt))
    id_cnt += 1

    item_metadata = \
        [(datatypes, "ConceptDatatype"),
         (classes, "ConceptClass"),
         (map_types, "ConceptMapType"),
         (ref_sources, "ConceptSource"),
         (ref_terms, "ConceptReferenceTerm", ref_sources)]
    for item_list in item_metadata:
        id_cnt = build_concept_metadata_mds_metadata_xml_items(list_el, id_cnt,
                                                               item_list)
    return list_el

def build_concept_metadata_mds_metadata_xml_items(list_el, id_cnt, item_list):
    concept_source_ids = dict()
    
    for key in item_list[0]:
        item_el = et.SubElement(list_el,
                                "org.openmrs." + item_list[1])
        item_el.set("id", str(id_cnt))
        id_cnt += 1
        item_el.set("uuid", key)
        for attrib_key in item_list[0][key].keys():
            if item_list[1] == "ConceptReferenceTerm":
                if attrib_key == "uuid":
                    continue
                elif attrib_key == "concept_source_id":
                    if item_list[0][key][attrib_key] not in \
                    concept_source_ids.keys():
                        for src_key in item_list[2]:
                            if item_list[2][src_key][attrib_key] == \
                            item_list[0][key][attrib_key]:
                                src_el = et.SubElement(item_el, \
                                                       "conceptSource")
                                src_el.set("id", str(id_cnt))
                                concept_source_ids[item_list[0][key]\
                                                   [attrib_key]] = id_cnt
                                id_cnt += 1
                                src_el.set("resolves-to", \
                                           "org.openmrs.ConceptSource")
                                src_el.set("uuid", item_list[2][src_key]\
                                           ["uuid"])
                                for src_att_key in item_list[2][src_key]\
                                .keys():
                                    if src_att_key == "uuid":
                                        continue
                                    src_att_el = et.SubElement(src_el, \
                                                    camel_case(src_att_key))
                                    src_att_el.text = \
                                    item_list[2][src_key][src_att_key]
                                break
                    else:
                        src_el = et.SubElement(item_el, "conceptSource")
                        src_el.set("reference",
                                   str(concept_source_ids[item_list[0][key]\
                                                          [attrib_key]]))
            el = et.SubElement(item_el, camel_case(attrib_key))
            el.text = item_list[0][key][attrib_key]

    return id_cnt

def build_ref_dict_items(ref_dicts, dict_item):
    for key in dict_item[0]:
        if dict_item[1] == "ReferenceTerm":
            ref_dicts[dict_item[1]][dict_item[0][key][dict_item[2]]] = \
                      ref_dicts["Source"][dict_item[0][key][dict_item[3]]] + \
                      ":" + dict_item[0][key][dict_item[4]]
        else:
            ref_dicts[dict_item[1]][dict_item[0][key][dict_item[2]]] = \
                      dict_item[0][key][dict_item[3]]
        
def build_ref_dicts(ref_dicts, datatypes, classes, map_types, ref_sources,
                    ref_terms):
    dict_metadata = \
        [(datatypes, "Datatype", "concept_datatype_id", "name"),
         (classes, "Class", "concept_class_id", "name"),
         (map_types, "MapType", "concept_map_type_id", "name"),
         (ref_sources, "Source", "concept_source_id", "name"),
         (ref_terms, "ReferenceTerm", "concept_reference_term_id",
          "concept_source_id", "code")]
    for dict_item in dict_metadata:
        if dict_item[1] not in ref_dicts.keys():
            ref_dicts[dict_item[1]] = dict()
        build_ref_dict_items(ref_dicts, dict_item)
    
def camel_case(st):
    output = ''.join(x for x in st.title() if x.isalnum())
    return output[0].lower() + output[1:]

def convert_concept_xml_to_csv(concept_xml_filenames, output_path,
                               concepts_output_path, concept_processing_order,
                               regexes, ref_dicts):
    ordered_xml_filenames = []
    for order_re in concept_processing_order:
        ordered_xml_filenames += [i for i in concept_xml_filenames
                                  if order_re.search(i)]

    for xml_filename in ordered_xml_filenames:
        concepts = dict()
        descriptions = dict()
        answers = dict()
        names = dict()
        ref_maps = dict()
        sets = dict()
    
        matches = regexes["concepts_filename"].match(xml_filename)
        name = matches.group(1)
        version = matches.group(2)
        
        tree = et.parse(output_path + "/" + xml_filename)

        for concept_el in tree.findall('concept'):
            if concept_el.get("concept_id") in concepts.keys():
                continue
            concepts[concept_el.get("concept_id")] = concept_el.attrib

        for description_el in tree.findall('concept_description'):
            if description_el.get("uuid") in descriptions.keys():
                continue
            descriptions[description_el.get("uuid")] = \
            description_el.attrib

        for answer_el in tree.findall('concept_answer'):
            if answer_el.get("uuid") in answers.keys():
                continue
            answers[answer_el.get("uuid")] = answer_el.attrib

        for name_el in tree.findall('concept_name'):
            if name_el.get("uuid") in names.keys():
                continue
            names[name_el.get("uuid")] = name_el.attrib

        for ref_map_el in tree.findall('concept_reference_map'):
            if ref_map_el.get("uuid") in ref_maps.keys():
                continue
            ref_maps[ref_map_el.get("uuid")] = ref_map_el.attrib

        for set_el in tree.findall('concept_set'):
            if set_el.get("uuid") in sets.keys():
                continue
            sets[set_el.get("uuid")] = set_el.attrib
    
        if regexes["main_concepts_file"].match(xml_filename):
            numeric_concepts = [i for i in concept_xml_filenames
                                if regexes["numeric_concepts"].search(i)][0]
            
            numeric_tree = et.parse(output_path + "/" + numeric_concepts)

            for numeric_el in numeric_tree.findall('concept_numeric'):
                concepts[numeric_el.get("concept_id")].\
                update(numeric_el.attrib)
            
            os.remove(output_path + '/' + numeric_concepts)

        concept_csv = dict()
        build_concept_csv(concept_csv, concepts, descriptions,
                          answers, names, ref_maps, sets, ref_dicts)
                
        header_data = []
        for key in concept_csv.keys():
            for csv_key in concept_csv[key].keys():
                if csv_key not in header_data:
                    header_data.append(csv_key)

        header_data.append("_version:" + version)
        header_data.append("_order:0")

        with open(concepts_output_path + "/" + name + ".csv", 'w',
                  newline="") as concept_csv_file:
            writer = csv.DictWriter(concept_csv_file, fieldnames=header_data,
                                    quoting=csv.QUOTE_NONNUMERIC)

            writer.writeheader()
            for key in concept_csv.keys():
                writer.writerow(concept_csv[key])
    
        os.remove(output_path + '/' + xml_filename)
            
def create_concept_metadata_mds_package(concept_xml_filenames, output_path,
                                        mds_output_path, regexes, ref_dicts):
    for xml_filename in concept_xml_filenames:
        if regexes["numeric_concepts"].match(xml_filename):
            continue

        datatypes = dict()
        classes = dict()
        map_types = dict()
        ref_sources = dict()
        ref_terms = dict()

        matches = regexes["concepts_filename"].match(xml_filename)
        name = matches.group(1).replace("_", " ")
        desc = "Standard set of " + matches.group(1).replace("_", " ") + \
            " distributed with the Reference Application"
        version = matches.group(2)
        mds_filename = matches.group(1) + "-" + version + ".zip"
        
        tree = et.parse(output_path + "/" + xml_filename)

        for datatype_el in tree.findall('concept_datatype'):
            if "Datatype" in ref_dicts.keys():
                if datatype_el.get("concept_datatype_id") in \
                ref_dicts["Datatype"].keys():
                    continue
            datatypes[datatype_el.get("uuid")] = datatype_el.attrib

        for class_el in tree.findall('concept_class'):
            if "Class" in ref_dicts.keys():
                if class_el.get("concept_class_id") in \
                ref_dicts["Class"].keys():
                    continue
            classes[class_el.get("uuid")] = class_el.attrib

        for map_type_el in tree.findall('concept_map_type'):
            if "MapType" in ref_dicts.keys():
                if map_type_el.get("concept_map_type_id") in \
                ref_dicts["MapType"].keys():
                    continue
            map_types[map_type_el.get("uuid")] = map_type_el.attrib

        for ref_source_el in tree.findall('concept_reference_source'):
#            if "Source" in ref_dicts.keys():
#                if ref_source_el.get("concept_source_id") in \
#                ref_dicts["Source"].keys():
#                    continue
            ref_sources[ref_source_el.get("uuid")] = ref_source_el.attrib

        for ref_term_el in tree.findall('concept_reference_term'):
            if "ReferenceTerm" in ref_dicts.keys():
                if ref_term_el.get("concept_reference_term_id") in \
                ref_dicts["ReferenceTerm"].keys():
                    continue
            ref_terms[ref_term_el.get("uuid")] = ref_term_el.attrib
    
        build_ref_dicts(ref_dicts, datatypes, classes, map_types, ref_sources,
                        ref_terms)

        header_xml_content = \
            build_concept_metadata_mds_header_xml(name, desc, version,
                                                  datatypes, classes,
                                                  map_types, ref_sources,
                                                  ref_terms)
        metadata_xml_content = \
            build_concept_metadata_mds_metadata_xml(datatypes, classes,
                                                    map_types, ref_sources,
                                                    ref_terms)

        xml_indent(header_xml_content)        
        xml_indent(metadata_xml_content)

        with zipfile.ZipFile(mds_output_path + "/" +
                             mds_filename, "w") as mds_zip:
            mds_zip.writestr("header.xml",
                             et.tostring(header_xml_content,
                                         encoding="unicode", method="xml"),
                             compress_type=zipfile.ZIP_DEFLATED)
            mds_zip.writestr("metadata.xml",
                             et.tostring(metadata_xml_content,
                                         encoding="unicode", method="xml"),
                             compress_type=zipfile.ZIP_DEFLATED)
    
def error(msg):
    print(msg)
    sys.exit()

def extract_concept_xml_files(rmd_omod_file, concept_xml_filenames,
                              output_path):
    with zipfile.ZipFile(rmd_omod_file) as rmd_omod_zip:
        for xml_filename in concept_xml_filenames:
            rmd_omod_zip.extract(xml_filename, path=output_path)

def extract_filenames_and_mds_jar(rmd_omod_file, mds_jar_filenames,
                                  concept_xml_filenames, output_path,
                                  use_pre_2x, regexes):

    with zipfile.ZipFile(rmd_omod_file) as rmd_omod_zip:
        mds_jar_filenames += list(filter(regexes["mds_jar"].match,
                                         rmd_omod_zip.namelist()))
        temp_xml_filenames = list(filter(regexes["concept_xml"].match,
                                         rmd_omod_zip.namelist()))
        rmd_omod_zip.extract(mds_jar_filenames[0], path=output_path)

        if use_pre_2x == 1:
            concept_xml_filenames += \
                [i for i in temp_xml_filenames
                 if not regexes["post_2x"].search(i)]
        else:
            concept_xml_filenames += \
                [i for i in temp_xml_filenames
                 if not regexes["pre_2x"].search(i)]            

def extract_mds_packages(mds_jar_filenames, output_path, mds_output_path,
                         regexes):
    mds_packages_list = []
    
    with zipfile.ZipFile(output_path + '/' +
                         mds_jar_filenames[0]) as mds_jar_zip:
        mds_packages_list += list(filter(regexes["mds_packages"].match,
                                         mds_jar_zip.namelist()))
        for mds_package in mds_packages_list:
            mds_jar_zip.extract(mds_package, path=mds_output_path)
    
    os.remove(output_path + '/' + mds_jar_filenames[0])
    os.rmdir(output_path + '/' + mds_jar_filenames[0].split("/", 1)[0])

def usage():
    print("""\
Usage: """ + sys.argv[0] + """
 [-h|--help] [--pre2x] [path to RMD .omod] [output dir]

optional arguments:
 -h, --help\t\tshow this help message and exit
 --pre2x\t\tif set, use the pre2.x numeric concepts file
\n\
required parameters:\n\
 [path to RMD .omod]\tpath to the Reference Metadata OMOD file
 [output dir]\t\tpath to write 'configuration' output directory
""")
    sys.exit(2)

def xml_indent(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            xml_indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i
            
#def xml_indent(elem, level=0):
#    i = "\n" + level*"  "
#    j = "\n" + (level-1)*"  "
#    if len(elem):
#        if not elem.text or not elem.text.strip():
#            elem.text = i + "  "
#        if not elem.tail or not elem.tail.strip():
#            elem.tail = i
#        for subelem in elem:
#            xml_indent(subelem, level+1)
#        if not elem.tail or not elem.tail.strip():
#            elem.tail = j
#    else:
#        if level and (not elem.tail or not elem.tail.strip()):
#            elem.tail = j
#    return elem        

def main(argv):
    use_pre_2x = 0
    
    try:
        opts, args = getopt.getopt(argv, 'h', ['help','pre2x'])
    except getopt.GetoptError:
        usage()

    for opt in opts:
        if opt[0] in ('-h', '--help'):
            usage()
        if opt[0] in ('--pre2x'):
            use_pre_2x = 1
        
    if not args or len(args) < 2:
        print(sys.argv[0] + ': required parameters missing\n')
        usage()

    output_path = args[1] + '/configuration'
    mds_output_path = output_path + '/metadatasharing'
    concepts_output_path = output_path + '/concepts'
    for path in [output_path, mds_output_path, concepts_output_path]:
        try:
            os.mkdir(path)
        except OSError:
            error("Could not create output directory: " + path)

    try:
        rmd_omod_file = open(args[0])
    except FileNotFoundError:
        error(args[0] + ': File does not exist')
    except IOError:
        error(args[0] + ': File is not accessible')
    finally:
        rmd_omod_file.close()

    mds_jar_filenames = []
    concept_xml_filenames = []
    
    regexes = dict([
    ("mds_packages", re.compile(".*\.zip$")),
    ("mds_jar", re.compile(".*referencemetadata-api-.*.jar$")),
    ("concept_xml", re.compile("^Reference_Application_.*.xml$")),
    ("pre_2x",
     re.compile("^Reference_Application_Numeric_Concepts-.*-pre2.x.xml$")),
    ("post_2x",
     re.compile("^Reference_Application_Numeric_Concepts-.*-2.x.xml$")),
    ("numeric_concepts",
     re.compile("^Reference_Application_Numeric_Concepts-.*.xml$")),
    ("concepts_filename", re.compile("^([A-Za-z_]+)-([0-9]+).xml$")),
    ("main_concepts_file",
     re.compile("^Reference_Application_Concepts-.*.xml$"))
    ])
    ref_dicts = dict()
    
    concept_processing_order = [
            re.compile("^Reference_Application_Concepts-.*.xml$"),
            re.compile("^Reference_Application_Diagnoses-.*.xml$"),
            re.compile("^Reference_Application_Order_Entry_.*.xml$")]

    extract_filenames_and_mds_jar(args[0], mds_jar_filenames,
                                  concept_xml_filenames, output_path,
                                  use_pre_2x, regexes)

    extract_mds_packages(mds_jar_filenames, output_path, mds_output_path,
                         regexes)

    extract_concept_xml_files(args[0], concept_xml_filenames, output_path)

    create_concept_metadata_mds_package(concept_xml_filenames, output_path,
                                        mds_output_path, regexes, ref_dicts)
    
    convert_concept_xml_to_csv(concept_xml_filenames, output_path,
                               concepts_output_path, concept_processing_order,
                               regexes, ref_dicts)

if __name__ == '__main__':
    main(sys.argv[1:])