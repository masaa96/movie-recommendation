from dataclasses import dataclass, field
from datetime import datetime
from typing import List


@dataclass
class Movie:
    _id: str
    title: str
    director: str
    year: int
    genres: List[str] = field(default_factory=list)
    cast: List[str] = field(default_factory=list)
    series: List[str] = field(default_factory=list)
    last_watched: datetime = None
    tags: List[str] = field(default_factory=list)
    description: str = None
    video_link: str = None
    cover_photo: str = None


@dataclass
class User:
    _id: str
    email: str
    password: str
    movies: List[str] = field(default_factory=list)


@dataclass
class Rating:
    _id: str
    user_id: str
    movie_id: str
    rating: int = 0
