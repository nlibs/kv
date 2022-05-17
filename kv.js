const SQLITE = require("sqlite");
const SCHEMA = {"data": {"columns": ["k", "v"], "index": [], "primary": "k" }}
const FLUSH_CHECK = 20 * 1000;
const MEMORY_CLEAR_FREQUENCY = 10 * 60 * 1000;

class KV
{
	constructor(path, is_binary)
	{
		var _this = this;
		this.is_binary = is_binary;
		if (typeof this.is_binary == "undefined")
			this.is_binary = false;

		this.cache = {};
		this.last_operation = new Map();
		this.dirty = new Set();
		this.deleted = new Set();

		this.db = new SQLITE(path, SCHEMA);
		setInterval(check, FLUSH_CHECK);
		
		function check()
		{
			_this.flush();
			_this.remove_infrequent_keys();
		}
	}

	set(k, v)
	{
		if (this.deleted.has(k))
			this.deleted.delete(k);

		this.last_operation.set(k, Date.now());
		this.dirty.add(k);
		if(this.is_binary)
			v = Buffer.from(v);

		this.cache[k] = v;
	}

	list_keys(prefix)
	{
		var list = [];
		var rows = this.db.read("SELECT k FROM data WHERE k LIKE (? || '%')", prefix);
		for(var key in this.cache)
		{
			if (key.startsWith(prefix))
				list.push(key);
		}

		for(var i=0;i<rows.length;i++)
			list.push(rows[i]["k"]);

		return Array.from(new Set(list));
	}

	get(k, default_value)
	{
		if (this.deleted.has(k))
			return default_value;

		this.last_operation.set(k, Date.now());
		var v = this.cache[k];
		if (typeof v != "undefined")
			return v;

		v = this.db.read("SELECT v FROM data WHERE k = ?", k)[0];
		
		if (typeof v == "undefined")
			return default_value;

		v = v["v"];
		if (!this.is_binary)
			v = JSON.parse(v);

		this.cache[k] = v;
		return v;
	}

	del(k)
	{
		this.deleted.add(k);
	}

	flush()
	{
		var _this = this;
		this.deleted.forEach(function(k)
		{
			delete _this.cache[k];
			_this.last_operation.delete(k);
			_this.dirty.delete(k);
			_this.db.fast_write("DELETE FROM data WHERE k = ?", k);
		});

		this.dirty.forEach(function(k)
		{
			var params = [k];
			
			if (!_this.is_binary)
				params.push(JSON.stringify(_this.cache[k]));
			else
				params.push(_this.cache[k]);

			_this.db.fast_write("REPLACE INTO data (k,v) VALUES (?,?)", params);
		})

		this.db.commit();
		this.dirty = new Set();
		this.deleted = new Set();
	}

	remove_infrequent_keys()
	{
		var _this = this;
		var now = Date.now();
		this.last_operation.forEach(function(v, k)
		{
			if (now - v > MEMORY_CLEAR_FREQUENCY)
			{
				_this.last_operation.delete(k);
				delete _this.cache[k];
			}
		});
	}
}

module.exports = KV;
