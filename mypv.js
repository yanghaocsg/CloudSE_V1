var HHLAnalytics;

(function() {
	BASE_URL = 'http://www.it300.com/',
    // Private array of chars to use
    CHARS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'.split(''),
	IS_IE = (navigator.userAgent.indexOf('MSIE') >= 0) && (navigator.userAgent.indexOf('Opera') < 0),
	BODY = document.getElementsByTagName('body')[0],

	getBrowser = function()	{
	    var ua = navigator.userAgent.toLowerCase();
	    if (ua.indexOf('msie') >= 0)
	    {
	        if (ua.indexOf('msie 7') >= 0)
	        {
	            return 'msie7';
	        }
	        else if (ua.indexOf('msie 8') >= 0)
	        {
	            return 'msie8';
	        }
	        else if (ua.indexOf('msie 6') >= 0)
	        {
	            return 'msie6';
	        }
	        else if (ua.indexOf('msie 5') >= 0)
	        {
	            return 'msie5';
	        }
	        return 'msie x';
	    }
	    else if (ua.indexOf('firefox') >= 0)
	    {
	        return 'firefox';
	    }
	    else if (ua.indexOf('applewebkit') >= 0)
	    {
	        if (ua.indexOf('chrome') >= 0)
	        {
	            return 'chrome';
	        }
	        return 'safari';
	    }
	    else if ( !! window.opera)
	    {
	        return 'opera';
	    }
	    else
	    {
	    	return 'unknown';
	    }
	},

	getOs = function() {
	    var sUserAgent = navigator.userAgent.toLowerCase();
	    if ((navigator.platform == "Win32") || (navigator.platform == "Windows"))
	    {
	        if (sUserAgent.indexOf("win95") > -1 || sUserAgent.indexOf("windows 95") > -1)
	        {
	            return "win95";
	        }
	        else if (sUserAgent.indexOf("win98") > -1 || sUserAgent.indexOf("windows 98") > -1)
	        {
	            return "win95";
	        }
	        else if (sUserAgent.indexOf("win 9x 4.90") > -1 || sUserAgent.indexOf("windows me") > -1)
	        {
	            return "winme";
	        }
	        else if (sUserAgent.indexOf("windows nt 5.0") > -1 || sUserAgent.indexOf("windows 2000") > -1)
	        {
	            return "win2000";
	        }
	        else if (sUserAgent.indexOf("windows nt 5.1") > -1 || sUserAgent.indexOf("windows xp") > -1)
	        {
	            return "winxp";
	        }
	        else if (sUserAgent.indexOf("windows nt 6.0") > -1 || sUserAgent.indexOf("windows vista") > -1)
	        {
	            return "winvista";
	        }
	        else if (sUserAgent.indexOf("windows nt 6.1") > -1 || sUserAgent.indexOf("windows 7") > -1)
	        {
	            return "win7";
	        }
	        else
	        {
	            return "winx";
	        }
	    }
	    else if ((navigator.platform == "Mac68K") || (navigator.platform == "MacPPC") || (navigator.platform == "Macintosh"))
	    {
	        return "mac";
	    }
	    else if ((navigator.platform == "X11"))
	    {
	        return "unix";
	    }
	    else
	    {
			return "unknown";
	    }
	},

	getLanguage = function() {
	    var language;
	    if (navigator.language)
	    {
	        language = navigator.language;
	    }
	    else if (navigator.browserLanguage)
	    {
	        language = navigator.browserLanguage;
	    }
	    else
	    {
	        language = '-';
	    }
		return language.toLowerCase();
	},

	getReferrer = function() {
		var top_referrer = 'HHL',
	    	parent_referrer = 'HHL',
	    	referrer = document.referrer;
		try {
			top_referrer = top.document.referrer;
		} catch (e){}

		try {
		    parent_referrer = window.parent.document.referrer;
		} catch (e){}
		
		if (parent_referrer !== 'HHL') {
		    referrer = parent_referrer;
		}

		if (top_referrer !== 'HHL') {
		    referrer = top_referrer;
		}
		return escape(referrer);
	},

	isFrame = function() {
		try {
			if (parent.location.href) {}
		}
		catch(e)
		{
			return true;
		}

		return false;
	},

	IS_FRAME = isFrame();

    if (HHLAnalytics == null)
    {
    	HHLAnalytics = {};
    }

	HHLAnalytics.init = function () {
		if (IS_IE && IS_FRAME)
		{
			try
			{
				BODY.addBehavior("#default#userData");
			}
			catch (e){}
		}

	    var str = '&r=' + getReferrer()
	    		  + '&b=' + getBrowser()
	    		  + '&o=' + getOs()
				  + '&l=' + document.location.href
	    		  + '&c=' + ((new Date()).getTime() + Math.floor(Math.random()*9999));

	    var _hpv_img = document.createElement('img');
	    _hpv_img.src = BASE_URL + 'hcount/index?' + str;
	};
})();

HHLAnalytics.init();