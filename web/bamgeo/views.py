from django.shortcuts import render_to_response

def home(request):
    docList = [1,2,3,4,5,6,7,8,9]
    return render_to_response('bamgeo/index.html', {'docList': docList})

def leaf(request):
    return render_to_response('bamgeo/leaf.html')