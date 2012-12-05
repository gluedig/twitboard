function handle_sort(event) {
	$("#leaderboard>li").tsort({attr:'score', order:'desc'});
	var children = $("#leaderboard").children();
	if ( children.length > results ) {
		var last = children.last();
		console.log("delete ", last.attr('score'), last);
		last.remove();
	}
};

function msg_move(data) {
 	var target = $("#"+data.user_id); 
  	var score = +data.score;

 	console.log("move ", score, target, data);

	target.attr('score', score);
	target.html('<div id="user_name">' + data.user_name + '</div><div id="score">' + score  + '</div>');
	
	$("#leaderboard").trigger('sort');
}

function add_new_row(data) {
	var new_data = new_li_data(data.user_id, data.score, data.user_name )
	$("#leaderboard").append(new_data);
	console.log("add ", data.score, $("#"+data.user_id), data);
	
	$("#leaderboard").trigger('sort');
};

function remove_row(data) {
	var target = $("#"+data.user_id);
	console.log("remove ", data.score, target, data);
	 
	target.remove();
	$("#leaderboard").trigger('sort');
};

function handle_sse_event(event, data) {
	var old_pos = +data.old_pos
	var new_pos = +data.new_pos
	if (old_pos != -1 && new_pos != -1) {
		msg_move(data);
	} else {
		if (old_pos != -1 && new_pos == -1)
			remove_row(data);
		
		if (old_pos == -1 && new_pos != -1)
			add_new_row(data);
	}
	
};

function sse_message(e) {
	var data = jQuery.parseJSON(e.data);
	if (data.msgtype == 'update')
	{
		$("#leaderboard").trigger('sse_message', data);
 	}
};

function new_li_data(user_id, score, user_name ) {
	var item = '<li id="' + user_id+ '" score="' + score + '"><div id="user_name">' + user_name + '</div><div id="score">' + score  + '</div></li>'
	return item;
}; 	

function refresh() {
  $.getJSON('/api/tag/'+ hashtag +'/'+results, function(data) {
  	  $("#leaderboard").empty();
  	  $.each(data['list'], function(key, val) {
   		var item = new_li_data(val['user_info']['user_id'], val['score'], val['user_info']['user_name']);
   		$("#leaderboard").append(item)
      });
      $("#leaderboard").trigger('sort');
  });
};