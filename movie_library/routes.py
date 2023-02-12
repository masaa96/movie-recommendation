import functools
import math
import uuid
import datetime
from dataclasses import asdict

from flask import (
    Blueprint,
    current_app,
    flash,
    redirect,
    render_template,
    session,
    url_for,
    request,
)
from flask_paginate import Pagination, get_page_parameter
from movie_library.forms import LoginForm, RegisterForm, MovieForm, ExtendedMovieForm
from movie_library.models import User, Movie, Rating
from passlib.hash import pbkdf2_sha256

from movie_library.recommend import get_recommendations

pages = Blueprint(
    "pages", __name__, template_folder="templates", static_folder="static"
)


def login_required(route):
    @functools.wraps(route)
    def route_wrapper(*args, **kwargs):
        if session.get("email") is None:
            return redirect(url_for(".login"))

        return route(*args, **kwargs)

    return route_wrapper


@pages.route("/")
@login_required
def index():
    page = request.args.get(get_page_parameter(), type=int, default=1)
    per_page = 20
    movie_data = current_app.db.movie.find()
    movies = [Movie(**movie) for movie in movie_data]
    paging_range = math.ceil(len(movies) / per_page)

    return render_template(
        "index.html",
        title="Movies Watchlist",
        movies_data=movies,
        page=page,
        per_page=per_page,
        movies_len=len(movies),
        paging_range=paging_range
    )


@pages.route("/register", methods=["POST", "GET"])
def register():
    if session.get("email"):
        return redirect(url_for(".index"))

    form = RegisterForm()

    if form.validate_on_submit():
        user = User(
            _id=uuid.uuid4().hex,
            email=form.email.data,
            password=pbkdf2_sha256.hash(form.password.data),
        )

        current_app.db.user.insert_one(asdict(user))

        flash("User registered successfully", "success")

        return redirect(url_for(".login"))

    return render_template(
        "register.html", title="Movies Watchlist - Register", form=form
    )


@pages.route("/login", methods=["GET", "POST"])
def login():
    if session.get("email"):
        return redirect(url_for(".index"))

    form = LoginForm()

    if form.validate_on_submit():
        user_data = current_app.db.user.find_one({"email": form.email.data})
        if not user_data:
            flash("Login credentials not correct", category="danger")
            return redirect(url_for(".login"))
        user = User(**user_data)

        if user and pbkdf2_sha256.verify(form.password.data, user.password):
            session["user_id"] = user._id
            session["email"] = user.email

            return redirect(url_for(".index"))

        flash("Login credentials not correct", category="danger")

    return render_template("login.html", title="Movies Watchlist - Login", form=form)


@pages.route("/logout")
def logout():
    del session["email"]
    del session["user_id"]

    return redirect(url_for(".login"))


@pages.route("/my-movies")
@login_required
def my_movies():
    user_data = current_app.db.user.find_one({"email": session["email"]})
    user = User(**user_data)

    movie_data = current_app.db.movie.find({"_id": {"$in": user.movies}})
    movies = [Movie(**movie) for movie in movie_data]

    return render_template(
        "my_movies.html",
        title="Movies Watchlist",
        movies_data=movies,
    )


@pages.route("/recommendations")
@login_required
def recommend():
    user_id = session["user_id"]

    rating_data = current_app.db.rating.find()
    ratings_list = [Rating(**rating) for rating in rating_data]
    movies_data = []

    movie_data = get_recommendations(ratings_list, user_id)
    for item in movie_data:
        movie_id = item.get("movie_id")
        movie = Movie(**current_app.db.movie.find_one({"_id": movie_id}))
        movies_data.append(movie)

    return render_template(
        "recommend.html",
        title="Recommended Movies",
        movies_data=movies_data
    )


@pages.route("/add", methods=["GET", "POST"])
@login_required
def add_movie():
    form = MovieForm()

    if form.validate_on_submit():
        movie = Movie(
            _id=uuid.uuid4().hex,
            title=form.title.data,
            director=form.director.data,
            year=form.year.data,
        )

        current_app.db.movie.insert_one(asdict(movie))

        current_app.db.user.update_one(
            {"_id": session["user_id"]}, {"$push": {"movies": movie._id}}
        )

        return redirect(url_for(".movie", _id=movie._id))

    return render_template(
        "new_movie.html", title="Movies Watchlist - Add Movie", form=form
    )


@pages.get("/movie/<string:_id>")
def movie(_id: str):
    movie = Movie(**current_app.db.movie.find_one({"_id": _id}))
    movie_rating_item = current_app.db.rating.find_one({
        "$and": [
            {"user_id": session["user_id"]},
            {"movie_id": _id}
        ]
    })
    movie_rating = movie_rating_item["rating"] if movie_rating_item else 0
    return render_template("movie_details.html", movie=movie, movie_rating=movie_rating)


@pages.route("/edit/<string:_id>", methods=["GET", "POST"])
@login_required
def edit_movie(_id: str):
    movie = Movie(**current_app.db.movie.find_one({"_id": _id}))
    form = ExtendedMovieForm(obj=movie)
    if form.validate_on_submit():
        movie.title = form.title.data
        movie.director = form.director.data
        movie.year = form.year.data
        movie.genres = form.genres.data
        movie.cast = form.cast.data
        movie.series = form.series.data
        movie.tags = form.tags.data
        movie.description = form.description.data
        movie.video_link = form.video_link.data
        movie.cover_photo = form.cover_photo.data

        current_app.db.movie.update_one({"_id": movie._id}, {"$set": asdict(movie)})
        return redirect(url_for(".movie", _id=movie._id))
    return render_template("movie_form.html", movie=movie, form=form)


@pages.get("/movie/<string:_id>/watch")
@login_required
def watch_today(_id):
    current_app.db.movie.update_one(
        {"_id": _id}, {"$set": {"last_watched": datetime.datetime.today()}}
    )

    return redirect(url_for(".movie", _id=_id))


@pages.get("/movie/<string:_id>/rate")
@login_required
def rate_movie(_id):
    rating = int(request.args.get("rating"))
    user_data = current_app.db.user.find_one({"email": session["email"]})
    user_id = session["user_id"]

    if _id in user_data["movies"]:
        current_app.db.rating.update_one(
            {"user_id": user_id, "movie_id": _id},
            {"$set": {"rating": rating}}
        )
    else:
        rating = Rating(
            _id=uuid.uuid4().hex,
            user_id=user_id,
            movie_id=_id,
            rating=rating
        )
        current_app.db.rating.insert_one(asdict(rating))
        current_app.db.user.update_one(
            {"_id": user_id}, {"$push": {"movies": _id}}
        )

    return redirect(url_for(".movie", _id=_id))


@pages.get("/toggle-theme")
def toggle_theme():
    current_theme = session.get("theme")
    if current_theme == "dark":
        session["theme"] = "light"
    else:
        session["theme"] = "dark"

    return redirect(request.args.get("current_page"))
