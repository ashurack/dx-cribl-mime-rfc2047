// TODO: Security considerations decoding?  Ex... Large strings, control characters, etc.

const libmime = require('libmime');
const { NestedPropertyAccessor } = C.expr;
const util = require('util');

// TODO: Use npm node-cache instead of relying on internal/undocumented classes
const NodeCache = C.internal.NodeCache;
const logger = C.util.getLogger('func:dx-mime-decode');

exports.disabled = 0;
exports.name = 'MIME Decode (RFC 2047)';
exports.version = '0.0.5';
exports.group = 'MIME Functions';

// NOTE: Skip/ignore config with a dstField of {value}. Also prevented in UI via conf.schema.json
const RESERVED_FIELDS = ["__channel", "__cloneCount", "__criblEventType", "__ctrlFields", "__FIN", "__final", "__hecToken", "__inputId", "__isBroken", "__packId", "__raw", "__srcIpPort", "cribl_breaker", "cribl_pipe", "cribl_route"]

// Performance logging
// TODO: Remove when done testing/documenting
const LOG_PERFORMANCE = false;

// Main config used
let _cache;
let items = [];

// Prevent event floods when exceptions are raised
let logMultiple = 1;
let processErrorCount = 0;
const maxLogMultiple = 1e5; // Log a minimum of 1:100_000 events

const logWithBackoff = (loggerMethod, msg, obj) => {
    // Suppress logging if processErrorCount is not a multiple of logMultiple
    // Will log errors 1, 2, 4, 8, 16, 32, etc.
    if (processErrorCount % logMultiple !== 0) return;
    
    loggerMethod(msg, obj ?? {});
      
    // Double the logMultiple up to a maximum threshold
    if (logMultiple < maxLogMultiple) logMultiple *= 2;

    // Start over if the next log entry will exceed MAX_SAFE_INTEGER
    if(processErrorCount + 1 > Number.MAX_SAFE_INTEGER)
        processErrorCount = 0;
};

exports.init = (opt) => {
    const conf = (opt || {}).conf || {};

    //["critical","error","warn","info","http","verbose","debug","silly","constructor","setBottleneck","setNewLevel", "isDebug","isSilly","passthru"]
    // ISSUE: Preview Log doesn't honor the configuration in Logging Channel Config (REQUEST-228)
    //logger.info('Logger info', {isDebug: logger.isDebug(), isSilly: logger.isSilly()})
    
    // Build config items to process
    items = (conf.processItems || []).filter(e => !e.disabled && e.srcField && e.dstField && !RESERVED_FIELDS.includes(e.dstField?.trim().replace(/'|"/g, ''))).map(item => ({
        srcField: new NestedPropertyAccessor(item.srcField?.trim()),
        dstField: new NestedPropertyAccessor(item.dstField?.trim()),
        override: item.override
    }));

    // Get skipped items and log: RESERVED_FIELDS
    // NOTE: Shouldn't find anything unless yml conf was updated directly
    const reservedItems = (conf.processItems || []).filter(e => !e.disabled && e.srcField && e.dstField && RESERVED_FIELDS.includes(e.dstField?.trim().replace(/'|"/g, ''))).map(item => ({
        srcField: new NestedPropertyAccessor(item.srcField?.trim()),
        dstField: new NestedPropertyAccessor(item.dstField?.trim()),
        override: item.override
    }));

    reservedItems.forEach((item) => {
        logger.warn('The destination field name cannot be a Cribl internal field', {dstField: item.dstField})
    })

    // Set cache if enabled
    if (conf.enableCache) {
        _cache = new NodeCache({
            stdTTL: conf.cacheTTL * 60 || 300,
            maxKeys: conf.maxCacheSize || 5000,
            checkperiod: 120,
            useClones: false,
        });
    }

    // Warmup for accurate pipeline diagnostics (lazy loading or JIT issue??)
    libmime.decodeWords('=?US-ASCII?Q?Warmup?=')
};

const unescapeCEF = (str) => {
    return str.replace(/\\([\\|=nr])/g, (_, escapedChar) => {
      switch (escapedChar) {
        case '\\':
          return '\\';  // \\
        case '=':
          return '=';   // \=
        case '|':
          return '|';   // \|
        case 'n':
          return '\n';  // \n
        case 'r':
          return '\r';  // \r
        default:
          return escapedChar; // catch-all
      }
    });
};

const _decode = (value) => {
    if (value == undefined) return null;

    //TODO: How to handle unexpected strings for decode? Return orig or ignore/exclude?
    if(typeof value !== 'string') return value;

    // TODO: Should this be done here or in an eval? Eval seems more expensive on pipeline diag.
    if (value.indexOf('\\') > -1)
        value = unescapeCEF(value);

    // Decode if cache disabled and return
    if(!_cache) return libmime.decodeWords(value);
    
    // Get cached value and return if it was a hit
    const cachedValue = _cache.get(value);
    if (cachedValue != undefined) return cachedValue;

    // Decode, set cache, return value
    const decodedValue = libmime.decodeWords(value);
    _cache.set(value, decodedValue);
    
    return decodedValue;
};

const processItem = (event, item) => {
    // TODO: Need to short-circuit error conditions or leave try/catch block in place to ensure event is returned by exports.process
    try {
        const srcFieldValue = item.srcField.get(event); 
        if (srcFieldValue == null) return;

        // Decode all items if srcFieldValue is an array
        const result = !Array.isArray(srcFieldValue) ? _decode(srcFieldValue) : srcFieldValue.map(v => _decode(v));
        
        // Return if result is unexpected (likely decode failure)
        if (result == null) return;
        
        // Get the value of the destination field
        const dstFieldValue = item.dstField.get(event);

        // Return if result is unexpected (likely decode failure)
        if (dstFieldValue == result) return;

        if (item.override) {
            item.dstField.set(event, result);
            return;
        }
            
        if (Array.isArray(dstFieldValue))
            item.dstField.set(event, !item.override ? [...dstFieldValue, ...result] : result);
        else
            item.dstField.set(event, !item.override && dstFieldValue ? [dstFieldValue, result] : result);

    } catch (err) {
        // TODO (maybe): Maintain separate 'processErrorCount' per item
        logWithBackoff(
            logger.warn,
            'Error processing item',
            { error: err.message, srcField: item?.srcField?.path, dstField: item?.dstField?.path, numErrors: ++processErrorCount, stack: err.stack }
          );
    }
};

const process_with_perf = (event) => {
    // NOTE: Performance logging was to test cache vs no-cache; may be worth it to leave as an option
    // TODO: Record/document performance output

    const tStart = performance.now();
    items.forEach(item => processItem(event, item));
    const tEnd = performance.now();

    if (items.length > 0)
        logger.silly('Total Time', {elapsed_ms: (tEnd - tStart).toFixed(4), cacheStatus: (_cache ? _cache.getStats() : null)});
}

const process_without_perf = (event) => {
    items.forEach(item => processItem(event, item));
}

exports.process = (event) => {
    if (!event) return event;
    
    if(LOG_PERFORMANCE)
        process_with_perf(event);
    else
        process_without_perf(event);
    
    return event;
};

exports.unload = () => {
    _cache = null;
};
