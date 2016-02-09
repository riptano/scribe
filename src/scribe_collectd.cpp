//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Bobby Johnson
// @author James Wang
// @author Jason Sobel
// @author Avinash Lakshman
// @author Anthony Giardullo

#include "common.h"
#include "scribe_collectd.h"

using namespace apache::thrift::concurrency;

using namespace facebook::fb303;
using namespace facebook;

using namespace scribe::thrift;
using namespace std;

using boost::shared_ptr;


#define DEFAULT_CHECK_PERIOD       5
#define DEFAULT_MAX_MSG_PER_SECOND 0
#define DEFAULT_MAX_QUEUE_SIZE     5000000LL
#define DEFAULT_SERVER_THREADS     3
#define DEFAULT_MAX_CONN           0

static string overall_category = "scribe_overall";
static string log_separator = ":";

scribeCollectd::scribeCollectd()
  :
    port(9999),
    checkPeriod(DEFAULT_CHECK_PERIOD),
    configFilename("/tmp/config.conf"),
    statusDetails("initial state"),
    numMsgLastSecond(0),
    maxMsgPerSecond(DEFAULT_MAX_MSG_PER_SECOND),
    maxConn(DEFAULT_MAX_CONN),
    maxQueueSize(DEFAULT_MAX_QUEUE_SIZE),
    newThreadPerCategory(true) {
  time(&lastMsgTime);
  scribeHandlerLock = scribe::concurrency::createReadWriteMutex();
}

scribeCollectd::~scribeCollectd() {
  deleteCategoryMap(categories);
  deleteCategoryMap(category_prefixes);
}





// Should be called while holding a writeLock on scribeHandlerLock
bool scribeCollectd::createCategoryFromModel(
  const string &category, const boost::shared_ptr<StoreQueue> &model) {

  // Make sure the category name is sane.
  try {
    string clean_path = boost::filesystem::path(category).string();

    if (clean_path.compare(category) != 0) {
      LOG_OPER("Category not a valid boost filename");
      return false;
    }

  } catch(const std::exception& e) {
    LOG_OPER("Category not a valid boost filename.  Boost exception:%s", e.what());
    return false;
  }

  boost::shared_ptr<StoreQueue> pstore;
  if (newThreadPerCategory) {
    // Create a new thread/StoreQueue for this category
    pstore = boost::shared_ptr<StoreQueue>(new StoreQueue(model, category));
    LOG_OPER("[%s] Creating new category store from model %s",
             category.c_str(), model->getCategoryHandled().c_str());

    // queue a command to the store to open it
    pstore->open();
  } else {
    // Use existing StoreQueue
    pstore = model;
    LOG_OPER("[%s] Using existing store for the config categories %s",
             category.c_str(), model->getCategoryHandled().c_str());
  }

  boost::shared_ptr<store_list_t> pstores;
  category_map_t::iterator cat_iter = categories.find(category);
  if (cat_iter == categories.end()) {
    pstores = boost::shared_ptr<store_list_t>(new store_list_t);
    categories[category] = pstores;
  } else {
    pstores = cat_iter->second;
  }
  pstores->push_back(pstore);

  return true;
}


// Check if we need to deny this request due to throttling
bool scribeCollectd::throttleRequest(const vector<LogEntry>&  messages) {
  // Check if we need to rate limit
  if (throttleDeny(messages.size())) {
    return true;
  }

  // Throttle based on store queues getting too long.
  // Note that there's one decision for all categories, because the whole array passed to us
  // must either succeed or fail together. Checking before we've queued anything also has
  // the nice property that any size array will succeed if we're unloaded before attempting
  // it, so we won't hit a case where there's a client request that will never succeed.
  // Also note that we always check all categories, not just the ones in this request.
  // This is a simplification based on the assumption that most Log() calls contain most
  // categories.
  unsigned long long max_count = 0;
  for (category_map_t::iterator cat_iter = categories.begin();
       cat_iter != categories.end();
       ++cat_iter) {
    boost::shared_ptr<store_list_t> pstores = cat_iter->second;
    if (!pstores) {
      throw std::logic_error("throttle check: iterator in category map holds null pointer");
    }
    for (store_list_t::iterator store_iter = pstores->begin();
         store_iter != pstores->end();
         ++store_iter) {
      if (*store_iter == NULL) {
        throw std::logic_error("throttle check: iterator in store map holds null pointer");
      } else {
        unsigned long long size = (*store_iter)->getSize();
        if (size > maxQueueSize) {
          return true;
        }
      }
    }
  }

  return false;
}

// Should be called while holding a writeLock on scribeHandlerLock
boost::shared_ptr<store_list_t> scribeCollectd::createNewCategory(
  const string& category) {

  boost::shared_ptr<store_list_t> store_list;

  // First, check the list of category prefixes for a model
  category_map_t::iterator cat_prefix_iter = category_prefixes.begin();
  while (cat_prefix_iter != category_prefixes.end()) {
    string::size_type len = cat_prefix_iter->first.size();
    if (cat_prefix_iter->first.compare(0, len-1, category, 0, len-1) == 0) {
      // Found a matching prefix model

      boost::shared_ptr<store_list_t> pstores = cat_prefix_iter->second;
      for (store_list_t::iterator store_iter = pstores->begin();
          store_iter != pstores->end(); ++store_iter) {
        createCategoryFromModel(category, *store_iter);
      }
      category_map_t::iterator cat_iter = categories.find(category);

      if (cat_iter != categories.end()) {
        store_list = cat_iter->second;
      } else {
        LOG_OPER("failed to create new prefix store for category <%s>",
                 category.c_str());
      }

      break;
    }
    cat_prefix_iter++;
  }


  // Then try creating a store if we have a default store defined
  if (store_list == NULL && !defaultStores.empty()) {
    for (store_list_t::iterator store_iter = defaultStores.begin();
        store_iter != defaultStores.end(); ++store_iter) {
      createCategoryFromModel(category, *store_iter);
    }
    category_map_t::iterator cat_iter = categories.find(category);
    if (cat_iter != categories.end()) {
      store_list = cat_iter->second;
    } else {
      LOG_OPER("failed to create new default store for category <%s>",
          category.c_str());
    }
  }

  return store_list;
}

// Add this message to every store in list
void scribeCollectd::addMessage(
  const LogEntry& entry,
  const boost::shared_ptr<store_list_t>& store_list) {

  int numstores = 0;

  // Add message to store_list
  for (store_list_t::iterator store_iter = store_list->begin();
       store_iter != store_list->end();
       ++store_iter) {
    ++numstores;
    boost::shared_ptr<LogEntry> ptr(new LogEntry);
    ptr->category = entry.category;
    ptr->message = entry.message;

    (*store_iter)->addMessage(ptr);
  }


}


ResultCode scribeCollectd::Log(const vector<LogEntry>&  messages) {
  ResultCode result = TRY_LATER;

  scribeHandlerLock->acquireRead();


  if (throttleRequest(messages)) {
    result = TRY_LATER;
    goto end;
  }

  for (vector<LogEntry>::const_iterator msg_iter = messages.begin();
       msg_iter != messages.end();
       ++msg_iter) {

    // disallow blank category from the start
    if ((*msg_iter).category.empty()) {
      continue;
    }

    boost::shared_ptr<store_list_t> store_list;
    string category = (*msg_iter).category;

    category_map_t::iterator cat_iter;
    // First look for an exact match of the category
    if ((cat_iter = categories.find(category)) != categories.end()) {
      store_list = cat_iter->second;
    }

    // Try creating a new store for this category if we didn't find one
    if (store_list == NULL) {
      // Need write lock to create a new category
      scribeHandlerLock->release();
      scribeHandlerLock->acquireWrite();

      // This may cause some duplicate messages if some messages in this batch
      // were already added to queues
      if(status == STOPPING) {
        result = TRY_LATER;
        goto end;
      }

      if ((cat_iter = categories.find(category)) != categories.end()) {
        store_list = cat_iter->second;
      } else {
        store_list = createNewCategory(category);
      }

    }

    if (store_list == NULL) {
      LOG_OPER("log entry has invalid category <%s>", category.c_str());

      continue;
    }

    // Log this message
    addMessage(*msg_iter, store_list);
  }

  result = OK;

 end:
  scribeHandlerLock->release();
  return result;
}

// Returns true if overloaded.
// Allows a fixed number of messages per second.
bool scribeCollectd::throttleDeny(int num_messages) {
  time_t now;
  if (0 == maxMsgPerSecond)
    return false;

  time(&now);
  if (now != lastMsgTime) {
    lastMsgTime = now;
    numMsgLastSecond = 0;
  }

  // If we get a single huge packet it's not cool, but we'd better
  // accept it or we'll keep having to read it and deny it indefinitely
  if (num_messages > (int)maxMsgPerSecond/2) {
    LOG_OPER("throttle allowing rediculously large packet with <%d> messages", num_messages);
    return false;
  }

  if (numMsgLastSecond + num_messages > maxMsgPerSecond) {
    LOG_OPER("throttle denying request with <%d> messages. It would exceed max of <%lu> messages this second",
           num_messages, maxMsgPerSecond);
    return true;
  } else {
    numMsgLastSecond += num_messages;
    return false;
  }
}

void scribeCollectd::stopStores() {
  boost::shared_ptr<store_list_t> store_list;
  for (store_list_t::iterator store_iter = defaultStores.begin();
      store_iter != defaultStores.end(); ++store_iter) {
    if (!(*store_iter)->isModelStore()) {
      (*store_iter)->stop();
    }
  }
  defaultStores.clear();
  deleteCategoryMap(categories);
  deleteCategoryMap(category_prefixes);

}

void scribeCollectd::shutdown() {
  RWGuard monitor(*scribeHandlerLock, true);
  stopStores();
  // calling stop to allow thrift to clean up client states and exit
  scribe::stopServer();
}

void scribeCollectd::reinitialize() {
  RWGuard monitor(*scribeHandlerLock, true);

  // reinitialize() will re-read the config file and re-configure the stores.
  // This is done without shutting down the Thrift server, so this will not
  // reconfigure any server settings such as port number.
  LOG_OPER("reinitializing");
  stopStores();
  initialize();
}

void scribeCollectd::initialize() {

  // This clears out the error state, grep for setStatus below for details

  bool perfect_config = true;
  bool enough_config_to_run = true;
  int numstores = 0;


  try {
    // Get the config data and parse it.
    // If a file has been explicitly specified we'll take the conf from there,
    // which is very handy for testing and one-off applications.
    // Otherwise we'll try to get it from the service management console and
    // fall back to a default file location. This is for production.
    StoreConf localconfig;
    string config_file;

    if (configFilename.empty()) {
      config_file = DEFAULT_CONF_FILE_LOCATION;
    } else {
      config_file = configFilename;
    }
    localconfig.parseConfig(config_file);
    // overwrite the current StoreConf
    config = localconfig;

    // load the global config
    config.getUnsigned("max_msg_per_second", maxMsgPerSecond);
    config.getUnsignedLongLong("max_queue_size", maxQueueSize);
    config.getUnsigned("check_interval", checkPeriod);
    if (checkPeriod == 0) {
      checkPeriod = 1;
    }
    config.getUnsigned("max_conn", maxConn);

    // If new_thread_per_category, then we will create a new thread/StoreQueue
    // for every unique message category seen.  Otherwise, we will just create
    // one thread for each top-level store defined in the config file.
    string temp;
    config.getString("new_thread_per_category", temp);
    if (0 == temp.compare("no")) {
      newThreadPerCategory = false;
    } else {
      newThreadPerCategory = true;
    }

    unsigned long int old_port = port;
    config.getUnsigned("port", port);
    if (old_port != 0 && port != old_port) {
      LOG_OPER("port %lu from conf file overriding old port %lu", port, old_port);
    }
    if (port <= 0) {
      throw runtime_error("No port number configured");
    }



    // Build a new map of stores, and move stores from the old map as
    // we find them in the config file. Any stores left in the old map
    // at the end will be deleted.
    std::vector<pStoreConf> store_confs;
    config.getAllStores(store_confs);
    for (std::vector<pStoreConf>::iterator iter = store_confs.begin();
         iter != store_confs.end();
         ++iter) {
        pStoreConf store_conf = (*iter);

        bool success = configureStore(store_conf, &numstores);

        if (!success) {
          perfect_config = false;
        }
    }
  } catch(const std::exception& e) {
    string errormsg("Bad config - exception: ");
    errormsg += e.what();
    perfect_config = false;
    enough_config_to_run = false;
  }

  if (numstores) {
    LOG_OPER("configured <%d> stores", numstores);
  } else {
    perfect_config = false;
    enough_config_to_run = false;
  }

  if (!enough_config_to_run) {
    // If the new configuration failed we'll run with
    // nothing configured and status set to WARNING
    deleteCategoryMap(categories);
    deleteCategoryMap(category_prefixes);
  }

}


// Configures the store specified by the store configuration. Returns false if failed.
bool scribeCollectd::configureStore(pStoreConf store_conf, int *numstores) {
  string category;
  boost::shared_ptr<StoreQueue> pstore;
  vector<string> category_list;
  boost::shared_ptr<StoreQueue> model;
  bool single_category = true;


  // Check if a single category is specified
  if (store_conf->getString("category", category)) {
    category_list.push_back(category);
  }

  // Check if multiple categories are specified
  string categories;
  if (store_conf->getString("categories", categories)) {
    // We want to set up to configure multiple categories, even if there is
    // only one category specified here so that configuration is consistent
    // for the 'categories' keyword.
    single_category = false;

    // Parse category names, separated by whitespace
    stringstream ss(categories);

    while (ss >> category) {
      category_list.push_back(category);
    }
  }

  if (category_list.size() == 0) {
    return false;
  }
  else if (single_category) {
    // configure single store
    boost::shared_ptr<StoreQueue> result =
      configureStoreCategory(store_conf, category_list[0], model);

    if (result == NULL) {
      return false;
    }

    (*numstores)++;
  } else {
    // configure multiple stores
    string type;

    if (!store_conf->getString("type", type) ||
        type.empty()) {
      string errormsg("Bad config - no type for store with category: ");
      errormsg += categories;
      return false;
    }

    // create model so that we can create stores as copies of this model
    model = configureStoreCategory(store_conf, categories, model, true);

    if (model == NULL) {
      string errormsg("Bad config - could not create store for category: ");
      errormsg += categories;
      return false;
    }

    // create a store for each category
    vector<string>::iterator iter;
    for (iter = category_list.begin(); iter < category_list.end(); iter++) {
      boost::shared_ptr<StoreQueue> result =
         configureStoreCategory(store_conf, *iter, model);

      if (!result) {
        return false;
      }

      (*numstores)++;
    }
  }

  return true;
}


// Configures the store specified by the store configuration and category.
boost::shared_ptr<StoreQueue> scribeCollectd::configureStoreCategory(
  pStoreConf store_conf,                       //configuration for store
  const string &category,                      //category name
  const boost::shared_ptr<StoreQueue> &model,  //model to use (optional)
  bool category_list) {                        //is a list of stores?

  bool is_default = false;
  bool already_created = false;

  if (category.empty()) {
    LOG_OPER("Bad config - store with blank category");
    return boost::shared_ptr<StoreQueue>();
  }

  LOG_OPER("CATEGORY : %s", category.c_str());
  if (0 == category.compare("default")) {
    is_default = true;
  }

  bool is_prefix_category = (!category.empty() &&
                             category[category.size() - 1] == '*' &&
                             !category_list);

  std::string type;
  if (!store_conf->getString("type", type) ||
      type.empty()) {
    string errormsg("Bad config - no type for store with category: ");
    errormsg += category;
    LOG_OPER(errormsg);
    return boost::shared_ptr<StoreQueue>();
  }

  // look for the store in the current list
  boost::shared_ptr<StoreQueue> pstore;

  try {
    if (model != NULL) {
      // Create a copy of the model if we want a new thread per category
      if (newThreadPerCategory && !is_default && !is_prefix_category) {
        pstore = boost::shared_ptr<StoreQueue>(new StoreQueue(model, category));
      } else {
        pstore = model;
        already_created = true;
      }
    } else {
      string store_name;
      bool is_model, multi_category, categories;

      /* remove any *'s from category name */
      if (is_prefix_category)
        store_name = category.substr(0, category.size() - 1);
      else
        store_name = category;

      // Does this store define multiple categories
      categories = (is_default || is_prefix_category || category_list);

      // Determine if this store will actually handle multiple categories
      multi_category = !newThreadPerCategory && categories;

      // Determine if this store is just a model for later stores
      is_model = newThreadPerCategory && categories;

      pstore =
        boost::shared_ptr<StoreQueue>(new StoreQueue(type, store_name, checkPeriod,
                                              is_model, multi_category));
    }
  } catch (...) {
    pstore.reset();
  }

  if (!pstore) {
    string errormsg("Bad config - can't create a store of type: ");
    errormsg += type;
    LOG_OPER(errormsg);
    return boost::shared_ptr<StoreQueue>();
  }

  // open store. and configure it if not copied from a model
  if (model == NULL) {
    pstore->configureAndOpen(store_conf);
  } else if (!already_created) {
    pstore->open();
  }

  if (category_list) {
    return (pstore);
  }
  if (is_default) {
    LOG_OPER("Creating default store");
    defaultStores.push_back(pstore);
  } else if (is_prefix_category) {
    boost::shared_ptr<store_list_t> pstores;
    category_map_t::iterator category_iter = category_prefixes.find(category);
    if (category_iter != category_prefixes.end()) {
      pstores = category_iter->second;
    } else {
      pstores = boost::shared_ptr<store_list_t>(new store_list_t);
      category_prefixes[category] = pstores;
    }
    pstores->push_back(pstore);
  } else if (!pstore->isModelStore()) {
    // push the new store onto the new map if it's not just a model
    boost::shared_ptr<store_list_t> pstores;
    category_map_t::iterator category_iter = categories.find(category);
    if (category_iter != categories.end()) {
      pstores = category_iter->second;
    } else {
      pstores = boost::shared_ptr<store_list_t>(new store_list_t);
      categories[category] = pstores;
    }
    pstores->push_back(pstore);
  }

  return pstore;
}


// delete everything in cats
void scribeCollectd::deleteCategoryMap(category_map_t& cats) {
  for (category_map_t::iterator cat_iter = cats.begin();
       cat_iter != cats.end();
       ++cat_iter) {
    boost::shared_ptr<store_list_t> pstores = cat_iter->second;
    if (!pstores) {
      throw std::logic_error("deleteCategoryMap: "
          "iterator in category map holds null pointer");
    }
    for (store_list_t::iterator store_iter = pstores->begin();
         store_iter != pstores->end();
         ++store_iter) {
      if (!*store_iter) {
        throw std::logic_error("deleteCategoryMap: "
            "iterator in store map holds null pointer");
      }

      if (!(*store_iter)->isModelStore()) {
        (*store_iter)->stop();
      }
    } // for each store
    pstores->clear();
  } // for each category
  cats.clear();
}

inline scribeCollectd* real(scribestruct *s) { return static_cast<scribeCollectd*>(s); }

scribestruct* new_scribe() {
  scribeCollectd *d = new scribeCollectd();
  d->initialize();

  return d;
}
void delete_scribe(scribestruct* s) { delete real(s); }
void scribe_log(scribestruct* s, char *log, char *category) {

  LogEntry le = LogEntry();
  le.__set_category(string(category));
  le.__set_message(string(log));

  vector<LogEntry>  messages;
  messages.push_back(le);

  real(s)->Log(messages);
  //LOG_OPER(string(log))
}
