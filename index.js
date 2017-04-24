'use strict';
var path = require('path');
var fs = require('fs');
var gutil = require('gulp-util');
var util = require('util');
var through = require('through2');
var Connection = require('ssh2');
var async = require('async');
var parents = require('parents');
var Stream = require('stream');
var assign = require('object-assign');


var finished = true;
var sftpCache = null; //sftp connection cache

/**
 *
 * @param path
 */
var normalizePath = function(path) {
    return path.replace(/\\/g, '/');
};

/**
 *
 * @param options
 * @returns {*}
 */
function getOptions(options) {
    options = assign({}, options);// credit sindresorhus

    if ( options.host === undefined ) {
        throw new gutil.PluginError('gulp-sftp', '`host` required.');
    }

    options.authKey = options.authKey || options.auth;
    var authFilePath = options.authFile || '.ftppass';
    var authFile = path.join('./',authFilePath);

    if ( options.authKey && fs.existsSync(authFile) ) {
        var auth = JSON.parse(fs.readFileSync(authFile,'utf8'))[options.authKey];
        if ( !auth ) {
            this.emit('error', new gutil.PluginError('gulp-sftp', 'Could not find authkey in .ftppass'));
        }
        if ( typeof auth == "string" && auth.indexOf(":") != -1 ) {
            var authparts = auth.split(":");
            auth          = { user: authparts[0], pass: authparts[1] };
        }
        for ( var attr in auth ) {
            options[attr] = auth[attr];
        }
    }

    //option aliases
    options.password = options.password||options.pass;
    options.username = options.username||options.user||'anonymous';

    /*
     * Lots of ways to present key info
     */
    options.key = options.key || options.keyLocation || null;
    if ( options.key && typeof options.key == "string" ) {
        options.key = { location: options.key };
    }

    //check for other options that imply a key or if there is no password
    if ( !options.key && (options.passphrase || options.keyContents || !options.password) ) {
        options.key = {};
    }

    if ( options.key ) {
        //aliases
        options.key.contents = options.key.contents || options.keyContents;
        options.key.passphrase = options.key.passphrase || options.passphrase;

        //defaults
        options.key.location = options.key.location||["~/.ssh/id_rsa","/.ssh/id_rsa","~/.ssh/id_dsa","/.ssh/id_dsa"];

        //type normalization
        if ( !util.isArray(options.key.location) ) {
            options.key.location = [options.key.location];
        }

        //resolve all home paths
        if ( options.key.location ) {
            var home = process.env.HOME || process.env.USERPROFILE;
            for ( var i = 0; i < options.key.location.length; i++ ) {
                if ( options.key.location[i].substr(0, 2) === '~/' ) {
                    options.key.location[i] = path.resolve(home, options.key.location[i].replace(/^~\//, ''));
                }
            }

            for ( var j = 0, keyPath; keyPath = options.key.location[j++]; ) {
                if ( fs.existsSync(keyPath) ) {
                    options.key.contents = fs.readFileSync(keyPath);
                    break;
                }
            }
        } else if ( !options.key.contents ) {
            this.emit('error', new gutil.PluginError('gulp-sftp', 'Cannot find RSA key, searched: ' + options.key.location.join(', ')));
        }
    }

    delete options.localPath;
    delete options.user;
    delete options.pass;
    delete options.logFiles;
    return options;
}

var connection = (function connection() {
    var conn = null;
    var persist_connection = false;
    var connection_options = {};

    function open(options, persist) {
        if ( conn ) {
            return conn;
        }

        connection_options = options || connection_options;
        persist_connection = persist || persist_connection;

        if ( !connection_options ) {
            this.emit('error', new gutil.PluginError('gulp-sftp', 'No connection options specified'));
        }

        gutil.log('Connection :: connecting');
        conn = new Connection();
        conn.connect(connection_options);
        gutil.log('Connection :: connected');

        conn.on('error', function (err) {
            this.emit('error', new gutil.PluginError('gulp-sftp', err));
        });

        conn.on('end', function () {
            gutil.log('Connection :: end');
        });

        conn.on('close', function (err) {
            if ( !finished ) {
                gutil.log('gulp-sftp', "SFTP abrupt closure");
                this.emit('error', new gutil.PluginError('gulp-sftp', "SFTP abrupt closure"));
            }
            if ( err ) {
                gutil.log('Connection :: close, ', gutil.colors.red('Error: ' + err));
            } else {
                gutil.log('Connection :: closed');
                if ( persist_connection ) {
                    gutil.log('Connection :: re-opening');
                    conn = null;
                    open();
                }
            }
        });

        return conn;
    }

    function close() {
        persist_connection = false;
        if ( conn ) {
            conn.end();
        }
    }

    function get() {
        return conn ? conn : false;
    }

    return {
        'open': open,
        'close': close,
        'get': get
    }
})();

function pool(remotePath, uploader) {
    if ( sftpCache ) {
        return uploader(sftpCache);
    }

    var c = connection.get();
    var upload = function() {
        gutil.log('Connection :: ready');
        c.sftp(function(err, sftp) {
            if (err) {
                throw err;
            }

            sftp.on('end', function() {
                gutil.log('SFTP :: SFTP session closed');
                sftpCache = null;
                if ( !finished ) {
                    this.emit('error', new gutil.PluginError('gulp-sftp', "SFTP abrupt closure"));
                }
            });

            sftpCache = sftp;
            uploader(sftpCache);
        });
    };
    if ( c._state == 'authenticated' ) {
        upload.apply(c);
    }
    c.on('ready', upload);
}

module.exports = function (options) {
    finished = false;
    var c = connection.get();
    if ( !c ) {
        options = getOptions(options);
        c = connection.connect(options);
    }

    var callback = options.callback || function () {};
    var fileCount = 0;
    var remotePath = options.remotePath || '/';
    var remotePlatform = options.remotePlatform || options.platform || 'unix';

    /*
     * End Key normalization, key should now be of form:
     * {location:Array,passphrase:String,contents:String}
     * or null
     */

    var logFiles = options.logFiles !== false;
    var mkDirCache = {};

    return through.obj(function (file, enc, cb) {
        if ( file.isNull() ) {
            this.push(file);
            return cb();
        }

        // have to create a new connection for each file otherwise they conflict, pulled from sindresorhus
        var finalRemotePath = normalizePath(path.join(remotePath, file.relative));

        // connection pulled from pool
        pool.call(this, finalRemotePath, function (sftp) {
            /* Create Directories */

            // get dir name from file path
            var dirname = path.dirname(finalRemotePath);
            // get parents of the target dir

            var fileDirs = parents(dirname)
                .map(function(d){return d.replace(/^\/~/,"~");})
                .map(normalizePath);

            if ( dirname.search(/^\//) === 0 ) {
                fileDirs = fileDirs.map(function (dir) {
                    if ( dir.search(/^\//) === 0 ) {
                        return dir;
                    }
                    return '/' + dir;
                });
            }

            // get filter out dirs that are closer to root than the base remote path
            // also filter out any dirs made during this gulp session
            fileDirs = fileDirs.filter(function (d) {
                return d.length >= remotePath.length && !mkDirCache[d];
            });

            // while there are dirs to create, create them
            // https://github.com/caolan/async#whilst - not the most commonly used async control flow
            async.whilst(function () {
                return fileDirs && fileDirs.length;
            }, function (next) {
                var d = fileDirs.pop();
                mkDirCache[d] = true;
                if ( remotePlatform && remotePlatform.toLowerCase().indexOf('win') !== -1 ) {
                    d = d.replace('/', '\\');
                }

                sftp.exists(d, function (exist) {
                    if ( !exist ) {
                        gutil.log('Remote directory does not exist, attempting to create', d);
                        sftp.mkdir(d, { mode: '0755' }, function (err) {
                            if ( err ) {
                                gutil.log('SFTP Mkdir Error:', gutil.colors.red(err + " " +d));
                            } else {
                                gutil.log('SFTP Created:', gutil.colors.green(d));
                            }
                            next();
                        });
                    } else {
                        next();
                    }
                });
            }, function () {
                var stream = sftp.createWriteStream(finalRemotePath, {
                    flags: 'w',
                    encoding: null,
                    mode: '0666',
                    autoClose: false
                });

                var uploadedBytes = 0;

                var highWaterMark = stream.highWaterMark||(16*1000);
                var size = file.stat.size;

                file.pipe(stream); // start upload

                stream.on('drain', function () {
                    uploadedBytes += highWaterMark;
                    gutil.log('gulp-sftp:', finalRemotePath, 'uploaded', (uploadedBytes / 1000) + 'kb');
                });

                stream.on('close', function (err) {
                    if ( err ) {
                        this.emit('error', new gutil.PluginError('gulp-sftp', err));
                    } else {
                        if ( logFiles ) {
                            gutil.log('gulp-sftp:', gutil.colors.green('Uploaded: ') + file.relative + gutil.colors.green(' => ') + finalRemotePath);
                        }

                        fileCount++;
                    }
                    return cb(err);
                });

            });
        });

        this.push(file);

    }, function (cb) {
        if ( fileCount > 0 ) {
            gutil.log('gulp-sftp:', gutil.colors.green(fileCount, fileCount === 1 ? 'file' : 'files', 'uploaded successfully'));
        } else {
            gutil.log('gulp-sftp:', gutil.colors.yellow('No files uploaded'));
        }
        finished = true;

        callback();
        cb();
    });
};

module.exports.connect = function (options) {
    /* Connection options, may be a key */
    var connection_options = {
        host : options.host,
        port : options.port || 22,
        username : options.username
    };

    if ( options.password ) {
        connection_options.password = options.password;
    } else if ( options.agent ) {
        connection_options.agent        = options.agent;
        connection_options.agentForward = options.agentForward || false;
    } else if ( key ) {
        connection_options.privateKey = options.key.contents;
        connection_options.passphrase = options.key.passphrase;
    }

    if ( options.timeout ) {
        connection_options.readyTimeout = options.timeout;
    }

    connection.open(connection_options, options.persist);
    return module.exports;
};

module.exports.close = function () {
    return connection.close();
};

module.exports.connection = function () {
    return connection.get();
};
